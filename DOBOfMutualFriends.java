import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class DOBOfMutualFriends {

	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
		String[] oa = new GenericOptionsParser(conf1, args).getRemainingArgs();
		if (oa.length != 5) {
			System.out.println(
					"Arguments should be in the form of: <Common_friends input_file_path> <user_data file_path> <output_path> <User_ID1> <User_ID2> ");
			System.exit(1);
		}

		conf1.set("InputFriend1", oa[3]);
		conf1.set("InputFriend2", oa[4]);
		conf1.set("Data", oa[1]);

		Job j1 = Job.getInstance(conf1, "Mutual Friends of A and B");
		j1.setMapOutputKeyClass(Text.class);
		j1.setMapOutputValueClass(Text.class);

		j1.setOutputKeyClass(Text.class);
		j1.setOutputValueClass(Text.class);
		
		j1.setJarByClass(DOBOfMutualFriends.class);
		j1.setMapperClass(mc.class);
		j1.setReducerClass(rc.class);
		FileInputFormat.addInputPath(j1, new Path(oa[0]));
		FileOutputFormat.setOutputPath(j1, new Path(oa[2]));

		if (!j1.waitForCompletion(true)) {
			System.exit(1);
		}
	}
	

	public static class mc extends
	Mapper<LongWritable, Text, Text, Text> {

		static HashMap<Integer, String> map = new HashMap<Integer, String>();

		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Path part = new Path(context.getConfiguration().get("Data"));

			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
			for (FileStatus s : fss) {
				String l;
				Path pt = s.getPath();
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				l = br.readLine();
				while (l != null) {
					String[] inf = l.split(",");
					if (inf.length == 10) {
						map.put(Integer.parseInt(inf[0]), inf[1] + ":" + inf[9]);
					}
					l = br.readLine();
				}
			}
		}

		public void map(LongWritable key, Text values, Context context) 
				throws IOException, InterruptedException {
			Configuration cnf1 = context.getConfiguration();
			int if1 = Integer.parseInt(cnf1.get("InputFriend1"));
			int if2 = Integer.parseInt(cnf1.get("InputFriend2"));
			String[] friends = values.toString().split("\t");
			if (friends.length == 2) {
				int friend2ID;

				int friend1ID = Integer.parseInt(friends[0]);
				String[] friendsList = friends[1].split(",");
				StringBuilder sb;;
				Text tuple_key = new Text();
				for (String friend2 : friendsList) {					
					friend2ID = Integer.parseInt(friend2);
					if ((friend1ID == if1 && friend2ID == if2)
							|| (friend1ID == if2 && friend2ID == if1)) {
						sb = new StringBuilder();
						if (friend1ID < friend2ID) {
							tuple_key.set(friend1ID + "," + friend2ID);
						} else {
							tuple_key.set(friend2ID + "," + friend1ID);
						}
						for (String friendInfo : friendsList) {
							int friendInfo2 = Integer.parseInt(friendInfo);
							sb.append(friendInfo2+":"+map.get(friendInfo2) + ",");
						}
						if (sb.length() > 0) {
							sb.deleteCharAt(sb.length() - 1);
						}
						context.write(tuple_key, new Text(sb.toString()));
					}
				}
			}
		}
	}

	public static class rc extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> friendsHashMap = new HashMap<String, Integer>();
			StringBuilder commonFriendLine = new StringBuilder();
			Text cf = new Text();
			for (Text tuples : values) {
				String[] fl = tuples.toString().split(",");
				for (String eachFriend : fl) {
					if (friendsHashMap.containsKey(eachFriend)) {
						String[] info = eachFriend.split(":");
						commonFriendLine.append(info[1]+":"+info[2]+ ",");
					} else {
						friendsHashMap.put(eachFriend, 1);
					}
				}
			}
			if (commonFriendLine.length() > 0) {
				commonFriendLine.deleteCharAt(commonFriendLine.length() - 1);
			}
			cf.set(new Text(commonFriendLine.toString()));
			context.write(key, cf);
		}
	}

}