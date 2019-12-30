package analysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class DataAnalysis extends Configured implements Tool {
	enum IntermediatePaths {
		COUNT
	}

	private static final Logger logger = LogManager.getLogger(DataAnalysis.class);

	public static class PathMapper extends Mapper<Object, Text, Text, Text> {
		private Text nodeX = new Text();
		private Text outgoing = new Text();

		private Text nodeY = new Text();
		private Text incoming = new Text();

		// input is X,Y where X follows Y
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String[] userIds = value.toString().split(",");
			String userX = userIds[0];
			String userY = userIds[1];

			// adding Max filter
			if (Integer.parseInt(userX) < Integer.MAX_VALUE && Integer.parseInt(userY) < Integer.MAX_VALUE) {
				nodeX.set(userX);
				outgoing.set("O" + userIds[1]);

				// for X -> Y, emit (X,"O"Y) as it is outgoing edge
				context.write(nodeX, outgoing);

				nodeY.set(userY);
				incoming.set("I" + userX);

				// for X->Y, emit (Y,"I"X) since it is incoming to Y from X
				context.write(nodeY, incoming);
			}
		}
	}

	public static class PathReducer extends Reducer<Text, Text, Text, LongWritable> {

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

			long incomingSum = 0;
			long outgoingSum = 0;

			// Separate edges whether incoming or outgoing
			for (Text t : values) {
				if (t.charAt(0) == 'I') {
					incomingSum += 1;
				} else if (t.charAt(0) == 'O') {
					outgoingSum += 1;
				}
			}

			// [X...]->Y->[Z...]
			// total number of paths of length 2 for Y are lengthX mulitplied by lengthZ
			long totalPathsForKey = incomingSum * outgoingSum;


			// Since we want count for all users, adding the totalpaths for that user to global counter.
			context.getCounter(IntermediatePaths.COUNT).increment(totalPathsForKey);
			LongWritable paths = new LongWritable(totalPathsForKey);

			context.write(key, paths);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Data Analysis");
		job.setJarByClass(DataAnalysis.class);

		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		conf.set("mapred.child.java.opts", "-Xmx8192m");

		job.setMapperClass(PathMapper.class);
		job.setReducerClass(PathReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		int jobCompleted = job.waitForCompletion(true) ? 0 : 1;
		Counter counter = job.getCounters().findCounter(IntermediatePaths.COUNT);

		// printing the total count of all length2 paths for all users
		// output is in syslog
		System.out.println(counter.getDisplayName() + ":" +counter.getValue());

		return jobCompleted;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new DataAnalysis(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}