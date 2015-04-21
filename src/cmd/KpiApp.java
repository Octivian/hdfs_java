package cmd;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KpiApp implements Tool{

	static  String INPUT_PATH = "";
	static  String OUT_PATH = "";
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new KpiApp(), args);
	}
	
	static class MyMapper extends Mapper<LongWritable,Text,Text,KpiWritable>{
		@Override
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, Text, KpiWritable>.Context context)
				throws IOException, InterruptedException {
			final String[] splited = v1.toString().split("\t");
			final String phone = splited[1];
			final KpiWritable kpiWritable = new KpiWritable(splited[6], splited[7], splited[8], splited[9]);
			context.write(new Text(phone), kpiWritable);
		}
	}
	
	
	static class MyReducer extends Reducer<Text,KpiWritable,Text,KpiWritable>{
		@Override
		protected void reduce(Text k2, Iterable<KpiWritable> v2s,
				Reducer<Text, KpiWritable, Text, KpiWritable>.Context context)
				throws IOException, InterruptedException {
			long upPackNum = 0L;
			long downPackNum = 0L;
			long upPayLoad = 0L;
			long downPayLoad = 0L;
			for (KpiWritable kpiWritable : v2s) {
				upPackNum += kpiWritable.upPackNum;
				downPackNum += kpiWritable.downPackNum;
				upPayLoad += kpiWritable.upPayLoad;
				downPayLoad += kpiWritable.downPayLoad;
			}
			final KpiWritable kpiWritable = new KpiWritable
					(Long.toString(upPackNum), Long.toString(downPackNum), 
							Long.toString(upPayLoad), Long.toString(downPayLoad));
			context.write(k2, kpiWritable);
		}
	}
	
	static class KpiParition extends HashPartitioner<Text, KpiWritable>{
		@Override
		public int getPartition(Text key, KpiWritable value, int numReduceTasks) {
			return key.toString().length() == 11?0:1;
		}
	}

	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int run(String[] args) throws Exception {
		INPUT_PATH = args[0];
		OUT_PATH = args[1];
		final Configuration conf = new Configuration();
		final Job job = new Job(conf, KpiApp.class.getSimpleName());
		job.setJarByClass(KpiApp.class);
		FileSystem fs = FileSystem.get(new URI(INPUT_PATH), conf);
		if(fs.exists(new Path(OUT_PATH))){
			fs.delete(new Path(OUT_PATH), true);
		}
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapperClass(MyMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(KpiWritable.class);
		
		job.setPartitionerClass(KpiParition.class);
		job.setNumReduceTasks(2);
		
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(KpiWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.waitForCompletion(true);
		return 0;
	}
}

class KpiWritable implements Writable{
	
	long upPackNum;
	long downPackNum;
	long upPayLoad;
	long downPayLoad;
	
	public KpiWritable(){}
	
	public KpiWritable(String upPackNum,String downPackNum,String upPayLoad,String downPayLoad){
		this.upPackNum = Long.parseLong(upPackNum);
		this.downPackNum = Long.parseLong(downPackNum);
		this.upPayLoad = Long.parseLong(upPayLoad);
		this.downPayLoad = Long.parseLong(downPayLoad);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upPackNum);
		out.writeLong(downPackNum);
		out.writeLong(upPayLoad);
		out.writeLong(downPayLoad);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.upPackNum = in.readLong();
		this.downPackNum = in.readLong();
		this.upPayLoad = in.readLong();
		this.downPayLoad = in.readLong();
	}
	
	@Override
	public String toString() {
		return upPackNum+"\t"+downPackNum+"\t"+upPayLoad+"\t"+downPayLoad;
	}
	
}
