package rj;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ReplicatedJoinTwitter extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(ReplicatedJoinTwitter.class);

	// Enum to setup a counter to count all the triangle values
	enum TriangleCount {
		COUNT
	}

	public static class TriangleCountMapper extends Mapper<Object, Text, Text, IntWritable> {

		// HashMap for each Map task to store the local copy of cache data
		private HashMap<String, Set<String>> XtoY = new HashMap<>();
		int MAX = 250000; // Max Filter to limit the input size

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			URI[] cacheFiles = context.getCacheFiles();

			if (cacheFiles != null && cacheFiles.length > 0) {
				String line;
				FileSystem fs;

				try {
					// Setting up FileSystem for distributed cache and reading the file
					fs = FileSystem.get(new URI(context.getConfiguration().get("cache.fs")), context.getConfiguration());
					Path path = new Path(cacheFiles[0].toString());

					BufferedReader reader = new BufferedReader(new InputStreamReader((fs.open(path))));
					while ((line = reader.readLine()) != null) {
						//for each line "user1, user2" splitting by , to get both users
						String[] userIds = line.split(",");
						String userX = userIds[0];
						String userY= userIds[1];

						// add user1 -> [user2, ...] to HashMap to they are within filter value
						// inorder to create a HashMap per map task for faster access.
						if(Integer.parseInt(userX) < MAX && Integer.parseInt(userY) < MAX) {
							Set<String> users;
							if (XtoY.containsKey(userX)) {
								users = XtoY.get(userX);
							} else {
								users = new HashSet<>();
							}
							users.add(userY);
							XtoY.put(userX, users);

							// for users X,Y if both X and Y are within filter but Y doesn't follow anybody.
							// hence there won't be a entry for Y. so add Y -> [empty] to HashMap
							if(!XtoY.containsKey(userY)) {
								XtoY.put(userY, new HashSet<>());
							}
						}
					}
				} catch (URISyntaxException e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String userIds[] = value.toString().split(",");
			String userX = userIds[0];
			String userY= userIds[1];

			if(Integer.parseInt(userX) < MAX && Integer.parseInt(userY) < MAX) {
				// lookup for key userY to get all Z's that Y follows from HashMap
				Set<String> usersZ = XtoY.get(userY);

				if(usersZ.size() > 0) {
					// for each userZ get all the userX that Z follows and check if and X is equal to userX
					// if it is then triangle is found and increment the global counter
					// Given X->Y, Get Y -> [Z..], for every Z, get Z -> [X...], check if X is same
					for (String userZ : usersZ) {
						Set<String> usersX = XtoY.get(userZ);
						if(usersX.size() > 0 && usersX.contains(userX)) {
							context.getCounter(TriangleCount.COUNT).increment(1);
						}
					}
				}
			}
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Replicated Join");
		job.setJarByClass(ReplicatedJoinTwitter.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");

		job.setMapperClass(TriangleCountMapper.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		// Setup cache file and file system for distributed cache.
		job.addCacheFile(new Path(args[0]+"/edges.csv").toUri());
		job.getConfiguration().set("cache.fs", "s3://hardiks-bucket/");

		// to run locally and on pseudo, comment the above line and uncomment the below line
//		job.getConfiguration().set("cache.fs", "hdfs://localhost:9000/");

		int r = job.waitForCompletion(true) ? 0 : 1;
		Counter counter = job.getCounters().findCounter(TriangleCount.COUNT);

		// Since, each triangle will be counted thrice, divide all by 3 and print the value
		// value is printed in Sysout
		System.out.println(counter.getDisplayName() + ":" +counter.getValue()/3);

		return r;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new ReplicatedJoinTwitter(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}