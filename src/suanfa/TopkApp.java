package suanfa;

import java.io.IOException;
import java.net.URI;

import mapred.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
/**
 * 
 * 此为求最大值
 * 
 * 求最大前100值
 *
 */
public class TopkApp {
	static final String INPUT_PATH = "hdfs://hadoop:9000/seq100w";
	static final String OUT_PATH = "hdfs://hadoop:9000/out";
	
	public static void main(String[] args) throws Exception {
		final Configuration conf = new Configuration();
		final Job job = new Job(conf,WordCount.class.getSimpleName());
		
		FileSystem fs = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fs.exists(new Path(OUT_PATH))){
			fs.delete(new Path(OUT_PATH), true);
		}
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);
		
		
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.waitForCompletion(true);
		
		
	}
	
	static class MyMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable>{
		long max = Long.MIN_VALUE;
		@Override
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, LongWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			final long temp = Long.parseLong(v1.toString());
			if(temp>max){
				max = temp;
			}
		}
		
		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, LongWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(new LongWritable(max), NullWritable.get());
		}
	}
	
	static class MyReducer extends Reducer<LongWritable, NullWritable,LongWritable, NullWritable>{
		long max = Long.MIN_VALUE;
		@Override
		protected void reduce(
				LongWritable k2,
				Iterable<NullWritable> values,
				Reducer<LongWritable, NullWritable, LongWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			final long temp = k2.get();
			if(temp>max){
				max = temp;
			}
		}
		
		@Override
		protected void cleanup(
				Reducer<LongWritable, NullWritable, LongWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(new LongWritable(max), NullWritable.get());
		}
	}
}
