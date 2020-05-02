import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;



public class Top10Friends {
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("The correct args format is: <Common_friends input_file_path> <temp_file_path> <output_path>");
			System.exit(1);
		}

		{
			Configuration conf1 = new Configuration();
			Job job1 = Job.getInstance(conf1, "Mutual Friends");
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);

			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			job1.setJarByClass(Top10Friends.class);
			job1.setMapperClass(m1.class);
			job1.setReducerClass(r1.class);

			

			FileInputFormat.addInputPath(job1, new Path(args[0]));
			FileOutputFormat.setOutputPath(job1, new Path(args[1]));

			if (!job1.waitForCompletion(true)) {
				System.exit(1);
			}

			{
				Configuration conf2 = new Configuration();
				Job job2 = Job.getInstance(conf2, "Top 10");
				job2.setMapOutputKeyClass(LongWritable.class);
				job2.setMapOutputValueClass(Text.class);

				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(LongWritable.class);
				job2.setJarByClass(Top10Friends.class);
				job2.setMapperClass(m2.class);
				job2.setReducerClass(r2.class);

				

				job2.setInputFormatClass(KeyValueTextInputFormat.class);

				job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

				job2.setNumReduceTasks(1);

				FileInputFormat.addInputPath(job2, new Path(args[1]));
				FileOutputFormat.setOutputPath(job2, new Path(args[2]));

				System.exit(job2.waitForCompletion(true) ? 0 : 1);
			}
		}
	}


	public static class m1 extends
	Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text values, Context context) 
				throws IOException, InterruptedException {
			String[] f = values.toString().split("\t");
			if (f.length == 2) {
				int f1ID = Integer.parseInt(f[0]);
				String[] fl = f[1].split(",");
				int f2ID;
				Text t_k = new Text();
				for (String f2 : fl) {
					f2ID = Integer.parseInt(f2);
					if (f1ID < f2ID) {
						t_k.set(f1ID + "," + f2ID);
					} else {
						t_k.set(f2ID + "," + f1ID);
					}
					context.write(t_k, new Text(f[1]));
				}
			}
		}
	}
	public static class m2 
	extends Mapper<Text, Text, LongWritable, Text> {

		private LongWritable count = new LongWritable();

		public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
			int n = Integer.parseInt(values.toString());
			count.set(n);
			context.write(count, key);
		}
	}

	public static class r1 
	extends Reducer<Text, Text, Text, IntWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> fh = new HashMap<String, Integer>();
			int nc = 0;
			for (Text tuples : values) {
				String[] fl = tuples.toString().split(",");
				for (String ef : fl) {
					if (fh.containsKey(ef)) {
						nc++;
					} else {
						fh.put(ef, 1);
					}
				}
			}
			context.write(key, new IntWritable(nc));
		}
	}


	public static class r2 extends 
	Reducer<LongWritable, Text, Text, LongWritable> {
		private int i = 0;

		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				if (i < 10) {
					i++;
					context.write(value, key);
				}
			}
		}
	}

	}