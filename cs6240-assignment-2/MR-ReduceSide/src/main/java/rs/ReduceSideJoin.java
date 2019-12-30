package rs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

// creating custom key
// Referred from the class slides
class Job2CustomKey implements WritableComparable<Job2CustomKey> {

	int from;
	int to;

	Job2CustomKey() {

	}

	Job2CustomKey(int from, int to) {
		this.from = from;
		this.to = to;
	}

	int getFrom() {
		return this.from;
	}

	int getTo() {
		return this.to;
	}

	void setFrom(int from) {
		this.from = from;
	}

	void setTo(int to) {
		this.to = to;
	}

	@Override
	public int compareTo(Job2CustomKey key) {
		if(key.from == this.from && key.to == this.to)
			return 0;
		if(key.from == this.from)
			return compare(this.to, key.to);
		return compare(this.from, key.from);
	}

	@Override  public int hashCode() {
		return this.from * 163 + this.to;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof Job2CustomKey) {
			Job2CustomKey ip = (Job2CustomKey) o;
			return this.from == ip.from && this.to == ip.to;
		}
		return false;
	}

	public static int compare(int attribute1, int attribute2) {
		return (attribute1 <= attribute2 ? -1 :1);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(from);
		dataOutput.writeInt(to);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.from = dataInput.readInt();
		this.to = dataInput.readInt();
	}
}

public class ReduceSideJoin extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(ReduceSideJoin.class);
	private static final String INTERMEDIATE_PATH = "intermediate_output";
	private static final int MAX = 50000;

	enum TriangleCount {
		COUNT
	}

	// Job1 Mapper
	public static class PathLength2Mapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			// Map receives object "userId1,userId2" i.e userId1 follows userId2
			String[] userIds = value.toString().split(",");
			String userX = userIds[0];
			String userY = userIds[1];

			// check if both users are within Max filter
			if (Integer.parseInt(userX) < MAX && Integer.parseInt(userY) < MAX) {
				Text outKey = new Text();
				Text outValue = new Text();

				outKey.set(userX);
				outValue.set("O" + userY);

				// for record, X,Y emit (X, "O"Y)
				// O represents outgoing  edge from X to Y
				context.write(outKey, outValue);

				// for record X,Y emit (Y, "I"X)
				// I represents incoming edges to Y from X
				outKey.set(userY);
				outValue.set("I" + userX);

				context.write(outKey, outValue);
				// For all users, X->Y and Y->Z will land in the same reducer where key is Y
			}
		}
	}

	// Job1 Reducer
	public static class PathLength2Reducer extends Reducer<Text, Text, NullWritable, Text> {
		private List<Text> listIncoming = new ArrayList<>();
		private List<Text> listOutgoing = new ArrayList<>();

		// The input to reducer is (Y, ["I"X, "O"Z, ...])
		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			listIncoming.clear();
			listOutgoing.clear();

			// separate the edges that Incoming or Outgoing
			for(Text t: values) {
				if(t.charAt(0) == 'O'){
					listOutgoing.add(new Text(t.toString().substring(1)));
				} else if(t.charAt(0) == 'I') {
					listIncoming.add(new Text(t.toString().substring(1)));
				}
			}

			// For each Z and each X emit (X,Y,Z) where X follows Y and Y follows Z
			// and Z and X cannot be equal
			for(Text Z: listOutgoing) {
				if(Z.getLength() > 0) {
					for(Text X: listIncoming) {
						if(X.getLength() > 0 && !X.toString().equals(Z.toString())) {
							context.write(NullWritable.get(), new Text(X+","+key.toString()+","+Z));
						}
					}
				}

			}

		}
	}

	// Job2 Mapper 1
	public static class Job2PathMapper extends Mapper<Object, Text, Job2CustomKey, Text> {

		// The input to Mapper is output of job1, i.e. X,Y,Z
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String[] pathUserIds = value.toString().split(",");

			int userX = Integer.parseInt(pathUserIds[0]);
			int userZ = Integer.parseInt(pathUserIds[2]);

			Job2CustomKey outKey = new Job2CustomKey(userX, userZ);
			Text outValue = new Text();

			// Since, we only want the count and not actualy triangle,
			// it is safe to only emit "Y" (to differentiate) and not actual Y user
			outValue.set("Y");

			// Since, the join is between X follows Z and Z follow X for triangle,
			// the key is a custom key with (X,Z)
			// the output for mapper 1 is ((X,Z), "Y")
			context.write(outKey,outValue);
		}
	}

	// Job2 Mapper 2
	public static class Job2EdgesMapper extends Mapper<Object, Text, Job2CustomKey, Text> {

		// The input to mapper Z,X (Z follows X) which is same as edges.csv
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			String userIds[] = value.toString().split(",");
			String userZ = userIds[0];
			String userX = userIds[1];

			if (Integer.parseInt(userX) < MAX && Integer.parseInt(userZ) < MAX) {
				int X = Integer.parseInt(userX);
				int Z = Integer.parseInt(userZ);

				// Joining using X,Z, hence using the same custom key
				Job2CustomKey outKey = new Job2CustomKey(X, Z);

				// the output for mapper 2 is ((X,Z), "E")
				context.write(outKey, new Text("E"));
			}
		}
	}

	// Job2 Reducer
	public static class Job2Reducer extends Reducer<Job2CustomKey, Text, NullWritable, Text> {
		int sum = 0;

		// The input to reducer is ((X,Z),["E","Y",....])
		@Override
		public void reduce(final Job2CustomKey key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			int edgesValues = 0;
			int pathValues = 0;

			// Separating both the values from mapper based on "E" and "Y"
			for(Text t: values) {
				if(t.charAt(0) == 'Y') {
					pathValues++;
				} else if (t.charAt(0) == 'E'){
					edgesValues++;
				}
			}

			// The total number of triangles will be Total number of Y's multiplied by total E's
			// X->[Y...]->Z and X->[E....]->Z
			sum += pathValues*edgesValues;
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// add the sum from each map task to the global counter
			context.getCounter(TriangleCount.COUNT).increment(sum);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		conf.set("mapred.child.java.opts", "-Xmx8192m");
		final Job job = Job.getInstance(conf, "Path length 2");
		job.setJarByClass(ReduceSideJoin.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");

		job.setMapperClass(PathLength2Mapper.class);
		job.setReducerClass(PathLength2Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// This program is a 2 Job MR pipeline.
		// The input to the first job is the edges.csv file
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// first job writes the output to a intermediate file
		FileOutputFormat.setOutputPath(job, new Path(INTERMEDIATE_PATH));

		job.waitForCompletion(true);

		final Job job2 = Job.getInstance(conf, "Join job");
		job2.setJarByClass(ReduceSideJoin.class);
		final Configuration jobConf2 = job2.getConfiguration();
		jobConf2.set("mapreduce.output.textoutputformat.separator", "\t");

		// The second job has two mappers as input.
		// the first mapper has input i.e the output of the job 1.
		// The second mapper has input as the edges.csv file
		MultipleInputs.addInputPath(job2, new Path(INTERMEDIATE_PATH), TextInputFormat.class, Job2PathMapper.class);
		MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class, Job2EdgesMapper.class);

		job2.setReducerClass(Job2Reducer.class);

		job2.setOutputKeyClass(Job2CustomKey.class);
		job2.setOutputValueClass(Text.class);

		// The output of the job2 is written to the output directory
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		int r = job2.waitForCompletion(true) ? 0 : 1;

		// The output is inside the global counter. since each triangle is counted thrice
		// the output is divided by 3.
		// The output is written in the Sysout file.
		Counter counter = job2.getCounters().findCounter(TriangleCount.COUNT);
		System.out.println(counter.getDisplayName() + ":" +counter.getValue()/3);

		return r;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new ReduceSideJoin(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}