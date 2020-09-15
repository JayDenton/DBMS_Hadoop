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

public class CalcTask{
	
	// Writable
	public static class AvgTuple implements Writable {
		private double average = 0;
		private long count = 1;
		private int state = 0;
		public Double getAverage() {
			return average;
		}
		public void setAverage(Double average) {
			this.average = average;
		}
		public long getCount() {
			return count;
		}
		public void setCount(long count) {
			this.count = count;
		}
		public int getState() {
			return state;
		}
		public void setState(int state) {
			this.state = state;
		}
		public void readFields(DataInput in) throws IOException {
			average = in.readDouble();
			count = in.readLong();
			state = in.readInt();
		}
		public void write(DataOutput out) throws IOException {
			out.writeDouble(average);
			out.writeLong(count);
			out.writeInt(state);
		}
		public String toString() {
			return state + "\t" + average + "\t" + count;
		}
	}
	
	// Map class
	public static class MapClass extends Mapper<LongWritable, Text, Text, AvgTuple>{
		private AvgTuple avgTuple = new AvgTuple();
		
		public void map(LongWritable key, Text value, Context context){
			try{
				// Parse Text
				String[] str = value.toString().split(",", -5);
				
				// Get values from Text
				double  wage = 0;
				int state = 0;
				if (str[72] != null && !str[72].isEmpty()){
					wage = Double.parseDouble(str[72]);
					state = Integer.parseInt(str[5]);
					
					// Ignore zero wage and null wage
					if (wage != 0) {
						// Wrap up AvgTuple
						avgTuple.setAverage(wage);
						avgTuple.setCount(1);
						avgTuple.setState(state);
						
						// Write pair <key is sex, out value is average_tuple>
						context.write(new Text(str[69]), avgTuple);
					}
				}
			}
			catch(Exception e){
				System.out.println(e.getMessage());
			}
		}
	}
	
	// Reducer class
	public static class ReduceClass extends Reducer<Text, AvgTuple, Text, AvgTuple>{
		private AvgTuple result = new AvgTuple();
		
		public void reduce(Text key, Iterable <AvgTuple> values, Context context
							) throws IOException, InterruptedException {
			double sum = 0;
			long count = 0;
			int state = 0;
			
			for (AvgTuple val : values){
				sum = sum + val.getAverage() * val.getCount();
				count = count + val.getCount();
				state = val.getState();
         	}
         	
         	result.setCount(count);
         	result.setAverage(sum/count);
         	result.setState(state);
         	
         	context.write(new Text(key), result);
		}
	}
	
	// Partitioner class
	public static class CalcPartitioner extends Partitioner <Text, AvgTuple>{
		public int getPartition(Text key, AvgTuple values, int numReduceTasks){
			int state = values.getState();
			
			if(numReduceTasks == 0){
				return 0;
			}
			
			// Seperate to different partitions according to different States
			if(state>=1 && state<=2){
				return state;
			}
			else if (state>=4 && state<=6){
				return state - 1;
			}
			else if (state>=8 && state<=13){
				return state - 2;
			}
			else if (state>=15 && state<=42){
				return state - 3;
			}
			else if (state>=44 && state<=51){
				return state - 4;
			}
			else if (state>=53 && state<=56){
				return state - 5;
			}
			else if (state==72){
				return 52;
			}
			// Not legit State
			else{
				return 0;
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		// Create a job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "calculate_average_wage2");
		job.setJarByClass(CalcTask.class);
		
		// File input and output
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Recursive travers files in the input path
		FileInputFormat.setInputDirRecursive(job, true);
		
		// Set Combiner
		job.setCombinerClass(ReduceClass.class);
		
		// Set Mapper
		job.setMapperClass(MapClass.class);
		
		//set Partitioner
		job.setPartitionerClass(CalcPartitioner.class);
		
		// Set Reducer
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(53);
		
		// Set key and value class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AvgTuple.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

