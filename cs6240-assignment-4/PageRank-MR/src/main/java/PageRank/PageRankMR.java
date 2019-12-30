package PageRank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

enum Counter {
	DELTA,
	PR_SUM
}

public class PageRankMR extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(PageRankMR.class);

	// Mapper class for iterating jobs.
	public static class PageRankMapper extends Mapper<Object, Text, IntWritable, Vertex> {
		double delta;
		int totalVertex;

		@Override
		public void setup(Context context) {

			// reads the delta value of previous iteration, sent by the driver. (initially 0.0)
			delta = Double.parseDouble(context.getConfiguration().get("delta"));
			totalVertex = Integer.parseInt(context.getConfiguration().get("totalVertex"));
		}

		@Override
		public void map(final Object key, final Text inputGraph, final Context context) throws IOException, InterruptedException {
			Vertex vertex = new Vertex(inputGraph);

            // Distributing the dangling node mass.
            double pageRank = vertex.getPageRank() + 0.85 * delta/totalVertex;

            Vertex newVertex = new Vertex(vertex.getId(), vertex.getAdjacencyList(),pageRank, -1);

            // passing the Graph structure along mappers to reducers.
            context.write(new IntWritable(vertex.getId()), newVertex);

            String[] adjacencyListVertices = vertex.getAdjacentVertices();
            double outlinkContribution = pageRank/adjacencyListVertices.length;

            // emit (id, contribution) for each node in the adjacency list.
            for(String adjacentVertex: adjacencyListVertices) {
            	if(!adjacentVertex.isEmpty()) {
					int vertexId = Integer.parseInt(adjacentVertex);
					context.write(new IntWritable(vertexId), new Vertex(-1, "[]", -1, outlinkContribution));
				}
            }
		}
	}

	// Reducer class for Iterating jobs.
	public static class PageRankReducer extends Reducer<IntWritable, Vertex, IntWritable, Vertex> {
		int totalVertex;

		@Override
		public void setup(Context context) {
			totalVertex = Integer.parseInt(context.getConfiguration().get("totalVertex"));
		}

		@Override
		public void reduce(final IntWritable key, final Iterable<Vertex> values, final Context context) throws IOException, InterruptedException {
			Vertex vertex = new Vertex();
			int id = Integer.parseInt(key.toString());
			double inlinkSum = 0.0;

			for(Vertex value: values) {
				if(value.getId() != -1) {
					// Recover the graph structure
					vertex = new Vertex(value.getId(), value.getAdjacencyList(),value.getPageRank(), -1);
				}
				else {
					// Add the inlink contribution for particular node.
					inlinkSum += value.getContribution();
				}
			}

			// calculate the new page rank using the formula.
			double newPageRank = calculateNewPageRank(id, totalVertex, inlinkSum);
			vertex.setNewPageRank(newPageRank);

			// emit(id,(list, pageRank)
			context.write(key, vertex);

			// Update the global counter with the page rank of dummy node
			// for passing it to next job.
			if (vertex.getId() == 0) {
				context.getCounter(Counter.DELTA).setValue((long) (inlinkSum * 10000000));
			}
		}
	}

	// Mapper class for the last job just for adding delta values.
	public static class DeltaMapper extends Mapper<Object, Text, IntWritable, Vertex> {
		double delta;
		int totalVertex;

		@Override
		public void setup(Context context) {

			// reading delta values of the previous job.
			delta = Double.parseDouble(context.getConfiguration().get("delta"));
			totalVertex = Integer.parseInt(context.getConfiguration().get("totalVertex"));
		}

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			Vertex vertex = new Vertex(value);
			double newPageRank = 0.0;

			// distributing delta value across the nodes.
			if(vertex.getId()!=0)
				newPageRank = vertex.getPageRank() + 0.85 * delta/totalVertex;

			// formatting the pageRank to  avoid scientific notation (eg: 1E-5)
			vertex.setNewPageRank(newPageRank);

			context.write(new IntWritable(vertex.getId()), vertex);

			context.getCounter(Counter.PR_SUM).increment((long) (newPageRank * 10000000));
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final int totalIterations = 10;
		final int k = 1000;
		int currentIteration = 1;

		String totalVertex = k * k+"";

		Path inputPath = new Path(args[0]);
		Path basePath = new Path(args[1]);

		Path outputPath = new Path(basePath, currentIteration + "");

		// Job 0
		Job job = getGenericJob(currentIteration, 0.0, totalVertex, inputPath, k);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);

		if(!job.waitForCompletion(true))
			return -1;

		currentIteration++;

		// Job 1 - 9
		while(currentIteration <= totalIterations) {

			// assigning output of previous iteration to next iteration.
			inputPath = outputPath;
			outputPath = new Path(basePath, currentIteration + "");

			// reading the delta values of the previous job and passing to the next job.
			double delta = (double)job.getCounters().findCounter(Counter.DELTA).getValue()/10000000;
			job = getGenericJob(currentIteration, delta, totalVertex, inputPath, k);

			FileInputFormat.setInputPaths(job, inputPath);
			FileOutputFormat.setOutputPath(job, outputPath);

			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			if(!job.waitForCompletion(true))
				return -1;
			currentIteration ++;
		}

		inputPath = outputPath;
		outputPath = new Path(basePath, "final");

		// Job 10. Last job to add just the delta values of the 10th iteration (converting it into long first)
		double delta = (double)job.getCounters().findCounter(Counter.DELTA).getValue()/10000000;
		job = getGenericJob(currentIteration, delta, totalVertex, inputPath, k);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapperClass(DeltaMapper.class);
		job.setNumReduceTasks(0);

		int completed = job.waitForCompletion(true) ? 0:1;

		System.out.println("Total Page Rank sum "+ (double)(job.getCounters().findCounter(Counter.PR_SUM).getValue())/10000000);
		return completed;
	}

	// method to create a generic job with all the parameters.
	private Job getGenericJob(int iteration, double delta, String totalVertex, Path inputPath, int k) throws IOException {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "PageRank MR"+iteration);
		job.setJarByClass(PageRankMR.class);

		final Configuration jobConf = job.getConfiguration();

		jobConf.set("mapreduce.output.textoutputformat.separator", ",");

		// setting the delta value to be sent through context.
		job.getConfiguration().set("delta", ""+delta);

		// using NLineInputFormat for creating 20 Map tasks
		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.addInputPath(job, inputPath);
		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", k*k/20);

		// sending the totalVertex in graph through context.
		job.getConfiguration().set("totalVertex", totalVertex);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Vertex.class);
		return job;
	}

	// method to calculate page rank using the formula.
	private static double calculateNewPageRank(int id, int totalVertex, double inlinkSum) {
		double newPageRank = 0.0;
		if(id!=0) {
			newPageRank = 0.15 * (1.0/totalVertex) + 0.85 * (inlinkSum);
		}
		return newPageRank;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new PageRankMR(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}

// Custom Vertex class to generate object to be sent from mappers tor reducers
// and reducer to hdfs
class Vertex implements Writable {

	private int id = -1;
	private String adjacencyList = "[]";
	private double pageRank = -1;
	private double contribution = -1;

	Vertex() { }

	Vertex(int id, String adjacencyList, double pageRank, double contribution) {
		this.id = id;
		this.adjacencyList = adjacencyList;
		this.pageRank = pageRank;
		this.contribution = contribution;
	}

	Vertex(Text inputVertex) {
		String graphVertex = inputVertex.toString();
		setId(graphVertex);
		setAdjacencyList(graphVertex);
		setPreviousPageRank(graphVertex);
		this.contribution = -1;
	}

	public void setId(String vertex) {
		String[] entities = vertex.split("[*?]|(,)");

		int id = Integer.parseInt(entities[0]);
		this.id = id;
	}

	public void setAdjacencyList(String vertex) {
		String[] entities = vertex.split("[*?]|(,)");

		String list = entities[1];
		this.adjacencyList = list;
	}

	public void setPreviousPageRank(String vertex) {
		String[] entities = vertex.split("[*?]|(,)");
		double oldPageRank = Double.parseDouble(entities[2]);
		this.pageRank = oldPageRank;
	}

	public void setNewPageRank(double newPagerank) {
		this.pageRank = newPagerank;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getId() {
		return this.id;
	}

	public String getAdjacencyList() {
		return this.adjacencyList;
	}

	public double getPageRank() {
		return this.pageRank;
	}

	public double getContribution() {
		return this.contribution;
	}

	public String[] getAdjacentVertices() {
		return this.adjacencyList.replace("[","").replace("]","").split(",");
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(id);
		dataOutput.writeUTF(adjacencyList);
		dataOutput.writeDouble(pageRank);
		dataOutput.writeDouble(contribution);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.id = dataInput.readInt();
		this.adjacencyList = dataInput.readUTF();
		this.pageRank = dataInput.readDouble();
		this.contribution = dataInput.readDouble();
	}

	@Override
	public String toString() {
		return ""+this.adjacencyList+","+String.format("%2.12f",this.pageRank);
	}
}