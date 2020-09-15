import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.util.*;

public class EquiWidth{
	
	// Writable
	public static class CountTuple implements Writable {
		private int ageGroup = 0;
		private long count = 1;
		public int getAgeGroup() {
			return ageGroup;
		}
		public void setAgeGroup(int ageGroup) {
			this.ageGroup = ageGroup;
		}
		public long getCount() {
			return count;
		}
		public void setCount(long count) {
			this.count = count;
		}
		public void readFields(DataInput in) throws IOException {
			ageGroup = in.readInt();
			count = in.readLong();
		}
		public void write(DataOutput out) throws IOException {
			out.writeInt(ageGroup);
			out.writeLong(count);
		}
		public String toString() {
			return ageGroup + "\t" + count;
		}
	}
	
	// Map class
	public static class MapClass extends Mapper<LongWritable, Text, Text, CountTuple>{
		private CountTuple countable = new CountTuple();
		
		public void map(LongWritable key, Text value, Context context){
			try{
				// Parse Text
				String[] str = value.toString().split(",", -5);
				
				// Get values from Text
				int age = Integer.parseInt(str[8]);
				
				// Wrap up CountTuple
				countable.setAgeGroup(age/10);
				countable.setCount(1);
				
				// Write pair <key is year, out value is CountTuple with age_group>
				context.write(new Text(str[0].substring(0,4)), countable);

			}
			catch(Exception e){
				System.out.println(e.getMessage());
			}
		}
	}
	
	// Reducer class
	public static class ReduceClass extends Reducer<Text, CountTuple, Text, CountTuple>{
		private CountTuple result = new CountTuple();
		
		public void reduce(Text key, Iterable <CountTuple> values, Context context
							) throws IOException, InterruptedException {
			long sum = 0;
			int ageGroup = -1;
			
			for (CountTuple val : values){
				sum = sum + val.getCount();
				ageGroup = val.getAgeGroup();
         	}
         	
         	result.setCount(sum);
         	result.setAgeGroup(ageGroup);
         	
         	context.write(new Text(key), result);
		}
	}
	
	// Partitioner class
	public static class AgePartitioner extends Partitioner <Text, CountTuple>{
		public int getPartition(Text key, CountTuple values, int numReduceTasks){
			
			if(numReduceTasks == 0){
				return 0;
			}
			else {
				return values.getAgeGroup();
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		// Create a job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "equi_width");
		job.setJarByClass(EquiWidth.class);
		
		// File input and output
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Recursive travers files in the input path
		FileInputFormat.setInputDirRecursive(job, true);
		
		// Set Mapper
		job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CountTuple.class);
		
		//set Partitioner
		job.setPartitionerClass(AgePartitioner.class);
		
		// Set Reducer
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(10);
		
		// Set key and value class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CountTuple.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

