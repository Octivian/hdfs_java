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
		
		//1.1�����ļ�����·��
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		//���������ļ���ʽ
		job.setInputFormatClass(TextInputFormat.class);
		
		//1.2�����Զ����MAPPER��
		job.setMapperClass(MyMapper.class);
		//MAP�������  ���<k3,v3>(reduce���������ֵ��)��������<k2,v2>��MAP���������ֵ�ԣ�����һ�£������ʡ��
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//1.3����(������Զ��������,������ݼ������ֻ��ź������Ž�reduce������д�벻ͬ���ļ���)
		job.setPartitionerClass(HashPartitioner.class);
		//����reduce�������
		job.setNumReduceTasks(1);
		
		//1.4���򣬷���
		
		//1.5��Լ
		
		//2.2ָ���Զ����REDUCE��
		job.setReducerClass(MyReducer.class);
		//ָ��REDUCE�������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//2.3�����ļ����·��
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		//ָ���ļ�����ĸ�ʽ����
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//job�ύ��JOBTRACKER����
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
