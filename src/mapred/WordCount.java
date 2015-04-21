package mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class WordCount {

	static final String INPUT_PATH = "hdfs://hadoop:9000/hello";
	static final String OUT_PATH = "hdfs://hadoop:9000/out";
	
	public static void main(String[] args) throws Exception {
		final Job job = new Job(new Configuration(),WordCount.class.getSimpleName());
		
		//1.1设置文件输入路径
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		//设置输入文件格式
		job.setInputFormatClass(TextInputFormat.class);
		
		//1.2设置自定义的MAPPER累
		job.setMapperClass(MyMapper.class);
		//MAP输出类型  如果<k3,v3>(reduce任务输出键值对)的类型与<k2,v2>（MAP任务输出键值对）类型一致，则可以省略
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//1.3分区(这里可自定义分区类,比如根据键区分手机号和座机号将reduce输出结果写入不同的文件中)
		job.setPartitionerClass(HashPartitioner.class);
		//设置reduce任务个数
		job.setNumReduceTasks(1);
		
		//1.4排序，分组
		
		//1.5归约
		
		//2.2指定自定义的REDUCE类
		job.setReducerClass(MyReducer.class);
		//指定REDUCE输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//2.3设置文件输出路径
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		//指定文件输出的格式化类
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//job提交给JOBTRACKER运行
		job.waitForCompletion(true);
		
		
	}
	
	static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		@Override
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			final String[] splited = v1.toString().split("\t");
			for (String word : splited) {
				context.write(new Text(word),new LongWritable(1));
			}
		}
	}
	
	static class MyReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2s,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long count = 0L;
			for (LongWritable longWritable : v2s) {
				count+=longWritable.get();
			}
			context.write(k2, new LongWritable(count));
		}
	}

}
