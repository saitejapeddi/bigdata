import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MaxAge {

	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
		String[] oa = new GenericOptionsParser(conf1, args).getRemainingArgs();
		if (oa.length != 5) {
			System.err.println("Arguments should be in the form of: <user and friends> <userdata.txt> <temppath_job1> <temppath_job2> <output_path>");
			System.exit(2);
		}
		conf1.set("Data", oa[1]);
		Job job1 = Job.getInstance(conf1, "max Age");
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Descending order");
		
		Configuration conf3 = new Configuration();
		conf3.set("Data", oa[1]);
		Job job3 = Job.getInstance(conf3, "Decreasing order");
		job1.setJarByClass(MaxAge.class);
		job1.setMapperClass(m1.class);
		job1.setReducerClass(r1.class);

		FileInputFormat.addInputPath(job1, new Path(oa[0]));
		FileOutputFormat.setOutputPath(job1, new Path(oa[2]));

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}



		job2.setJarByClass(MaxAge.class);
		job2.setMapperClass(m2.class);
		job2.setReducerClass(r2.class);

		FileInputFormat.addInputPath(job2, new Path(oa[2]));
		FileOutputFormat.setOutputPath(job2, new Path(oa[3]));

		job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}


		job3.setJarByClass(MaxAge.class);
		job3.setMapperClass(m3.class);
		job3.setReducerClass(r3.class);

		FileInputFormat.addInputPath(job3, new Path(oa[3]));
		FileOutputFormat.setOutputPath(job3, new Path(oa[4]));

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		if (!job3.waitForCompletion(true)) {
			System.exit(1);
		}
	}

	public static class m1 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] friends = values.toString().split("\t");
			if (friends.length == 2) {
				context.write(new Text(friends[0]), new Text(friends[1]));
			}
		}
	}
	public static class m2 extends Mapper<LongWritable, Text, LongWritable, Text> {
		private LongWritable count = new LongWritable();

		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] inf = values.toString().split("\t");
			if (inf.length == 2) {
				count.set(Long.parseLong(inf[1]));
				context.write(count, new Text(inf[0]));
			}
		}
	}
	public static class m3 extends Mapper<LongWritable, Text, Text, Text> {
		private int c = 0;

		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			if (c < 10) {
				String[] info = values.toString().split("\t");
				if (info.length == 2) {
					c++;
					context.write(new Text(info[0]), new Text(info[1]));
				}
			}
		}
	}
	public static class r1 extends Reducer<Text, Text, Text, Text> {

		private int calculateAge(String s) throws ParseException {

			Calendar today = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat("mm/dd/yyyy");
			Date date = sdf.parse(s);
			Calendar dob = Calendar.getInstance();
			dob.setTime(date);

			int curYear = today.get(Calendar.YEAR);
			int dobYear = dob.get(Calendar.YEAR);
			int age = curYear - dobYear;

			int curMonth = today.get(Calendar.MONTH);
			int dobMonth = dob.get(Calendar.MONTH);
			if (dobMonth > curMonth) { 
				age--;
			} else if (dobMonth == curMonth) { 
				int curDay = today.get(Calendar.DAY_OF_MONTH);
				int dobDay = dob.get(Calendar.DAY_OF_MONTH);
				if (dobDay > curDay) { 
					age--;
				}
			}
			return age;
		}

		static HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();

		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path part = new Path(conf.get("Data"));
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
			for (FileStatus status : fss) {
				Path pt = status.getPath();
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String l;
				l = br.readLine();
				while (l != null) {
					String[] info = l.split(",");
					if (info.length == 10) {
						try {
							int age = calculateAge(info[9]);
							map.put(Integer.parseInt(info[0]), age);
						} catch (ParseException e) {
							e.printStackTrace();
						}
					}
					l = br.readLine();
				}
			}
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text tuples : values) {
				String[] friendsList = tuples.toString().split(",");
				int maxAge = -1;
				int age;
				for (String eachFriend : friendsList) {
					age = map.get(Integer.parseInt(eachFriend));
					if (age > maxAge) {
						maxAge = age;
					}
				}
				context.write(key, new Text(Integer.toString(maxAge)));
			}
		}
	}

	
	public static class r2 extends Reducer<LongWritable, Text, Text, Text> {
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text t : values) {
				context.write(t, new Text(Long.toString(key.get())));
			}
		}
	}
	


	
	public static class r3 extends Reducer<Text, Text, Text, Text> {
		static HashMap<Integer, String> map = new HashMap<Integer, String>();

		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path part = new Path(conf.get("Data"));
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
			for (FileStatus status : fss) {
				Path pt = status.getPath();
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				line = br.readLine();
				while (line != null) {
					String[] info = line.split(",");
					if (info.length == 10) {
						map.put(Integer.parseInt(info[0]),info[0]+"\t"+ info[3]+","+info[4]+","+info[5]);
					}
					line = br.readLine();
				}
			}
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int userId = Integer.parseInt(key.toString());
			String userInfo = map.get(userId);
			for (Text tuples : values) {
				context.write(new Text(userInfo), tuples);
			}
		}
	}
	
	

}