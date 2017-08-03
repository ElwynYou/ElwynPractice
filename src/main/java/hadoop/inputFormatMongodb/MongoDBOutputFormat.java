package hadoop.inputFormatMongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import mongoDB.MongoUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.bson.Document;

import java.io.IOException;

/**
 * @Package hadoop.inputFormatMongodb
 * @Description: //自定义outputformat
 * @Author elwyn
 * @Date 2017/8/1 23:43
 * @Email elonyong@163.com
 */
public class MongoDBOutputFormat<V extends MongoDBWritable> extends OutputFormat<NullWritable, V> {
	public MongoDBOutputFormat() {
		super();
	}


	@Override
	public RecordWriter<NullWritable, V> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		return new MongoDBRecordWriter<>(taskAttemptContext);
	}

	@Override
	public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		return new FileOutputCommitter(null, taskAttemptContext);
	}
}
