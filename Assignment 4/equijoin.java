// code by Tirth

import java.io.IOException;
import java.util.Vector;
import java.util.Iterator;

//apache imports

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class equijoin {
	// Mapper class
	
	public static class equiJoinMapper extends Mapper<Object, Text, Text, Text> {

		//Map method Input: key,values -> returns : List<key,values>
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().trim(); //remove unnecessary space
			String line_list[] = line.split(",");
			
			//second column in line is join attribute
			Text join_key = new Text(line_list[1]); 
			context.write(join_key, value);
		}
	}
	
	//Reducer class

	public static class equiJoinReducer extends Reducer<Text, Text, Text, Text> {
		
		//Reduce method Input : <key, list[values]> -> list<key,values>
		
		public void reduce(Text key, Iterable<Text> content, Context context) throws IOException, InterruptedException {
			Vector<String> list_A = new Vector<String>();
			Vector<String> list_B = new Vector<String>();
			String ans_string = "";
			Text ans_set = new Text();
			Iterator<Text> values = content.iterator();
			String string = values.next().toString();
			String val = string.trim();
			
			//Adding 1st attribute value which is partition name of first line
			String partition_name1 = val.split(",")[0]; 
			list_A.add(string);
				
			while (values.hasNext()) {
				String value = values.next().toString();
				if (value.trim().split(",")[0].equals(partition_name1))
					list_A.add(value);
				else
					list_B.add(value);
			}

			for (int i = 0; i < list_A.size(); i++) {
				for (int j = 0; j < list_B.size(); j++) {
					ans_string = list_A.get(i) + ", " + list_B.get(j);
					ans_set.set(ans_string);
					context.write(null, ans_set);
					ans_set.clear();
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		Job my_job = Job.getInstance(config, "equijoin");
		my_job.setJarByClass(equijoin.class);
		my_job.setMapperClass(equiJoinMapper.class);
		my_job.setReducerClass(equiJoinReducer.class);
		my_job.setOutputKeyClass(Text.class);
		my_job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(my_job, new Path(args[0]));
		FileOutputFormat.setOutputPath(my_job, new Path(args[1]));
		System.exit(my_job.waitForCompletion(true) ? 0 : 1);
	}
}
