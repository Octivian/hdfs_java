package hmbbs_logs;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KpiLogs extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new KpiLogs(), args);
	}

	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, this.getClass().getSimpleName());
		job.setJarByClass(KpiLogs.class);
		FileInputFormat.setInputPaths(job, args[0]);
		job.setMapperClass(MyMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		return 0;
	}
	
	static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		LogParser logParser = new LogParser();
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String [] tmpValue = logParser.parse(value.toString());
			if(tmpValue[2].startsWith("GET /static/")){
				return;
			}
			//过掉开头的特定格式字符串
			if(tmpValue[2].startsWith("GET /")){
				tmpValue[2] = tmpValue[2].substring("GET /".length());
			}
			else if(tmpValue[2].startsWith("POST /")){
				tmpValue[2] = tmpValue[2].substring("POST /".length());
			}
			
			//过滤结尾的特定格式字符串
			if(tmpValue[2].endsWith(" HTTP/1.1")){
				tmpValue[2] = tmpValue[2].substring(0, tmpValue[2].length()-" HTTP/1.1".length());
			}
			value.set(tmpValue[0]+"\t"+tmpValue[1]+"\t"+tmpValue[2]+"\t"+tmpValue[3]+"\t"+tmpValue[4]);
			context.write(key, value);
		}
	}
	
	static class MyReducer extends Reducer<LongWritable, Text, Text, NullWritable>{
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Reducer<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			for (Text text : values) {
				context.write(text, NullWritable.get());
			}
		}
	}
	
	static class LogParser{
		private static final SimpleDateFormat FORMAT = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
		public static final SimpleDateFormat dateformat1=new SimpleDateFormat("yyyy_MM_dd HH:mm:ss");
		public String [] parse(String value){
			return new String[]{parseIp(value),parseTime(value),parseUrl(value),parseStatus(value),parseFlow(value)};
		}
		private String parseIp(String value){
			return value.split(" - - ")[0].trim();
		}
		private String parseTime(String value){
			int firstPos = value.indexOf("[");
			int lastPos = value.lastIndexOf("]");
			String timeString = value.substring(firstPos+1, lastPos);
			Date time = null;
			try {
				time = FORMAT.parse(timeString);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			return dateformat1.format(time);
		}
		private String parseUrl(String value){
			int firstPos = value.indexOf("\"");
			int lastPos = value.lastIndexOf("\"");
			return value.substring(firstPos+1, lastPos);
		}
		private String parseStatus(String value){
			final String trim = value.substring(value.lastIndexOf("\"")+1).trim();
			String status = trim.split(" ")[0];
			return status;
		}
		private String parseFlow(String value){
			final String trim = value.substring(value.lastIndexOf("\"")+1).trim();
			String traffic = trim.split(" ")[1];
			return traffic;
		}
	}

}
