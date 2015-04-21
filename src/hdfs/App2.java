package hdfs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 使用FileSystem
 * @author Administrator
 *
 */
public class App2 {
	public static final String HDFS_PATH="hdfs://hadoop:9000";
	public static final String DIR_PATH="/d1000";
	public static final String FILE_PATH="/d1000/f1000";

	public static void main(String[] args) throws Exception{
		final FileSystem fs = FileSystem.get(new URI(HDFS_PATH), new Configuration());
	//创建文件夹
//		makeDirectory(fs);
		//上传文件
//		uploadData(fs);
	//下载文件
//		downloadData(fs);
	//删除文件（夹）
//		deleteFile(fs);
	}

	private static void deleteFile(final FileSystem fs) throws IOException {
		fs.delete(new Path(FILE_PATH), true);
	}

	private static void downloadData(final FileSystem fs) throws IOException {
		final FSDataInputStream in = fs.open(new Path(FILE_PATH));
		IOUtils.copyBytes(in, System.out, 1024,true);
	}

	private static void makeDirectory(final FileSystem fs) throws IOException {
		fs.mkdirs(new Path(DIR_PATH));
	}

	private static void uploadData(final FileSystem fs) throws IOException,
			FileNotFoundException {
		final FSDataOutputStream out = fs.create(new Path(FILE_PATH));
		final FileInputStream in = new FileInputStream("C:/Users/Administrator/Desktop/2.txt");
		IOUtils.copyBytes(in, out,1024 ,true);
	}
	
}
