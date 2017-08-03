package hadoop.inputFormatMongodb;

import com.mongodb.client.MongoCollection;
import mongoDB.MongoUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @Name hadoop.inputFormatMongodb.MongoDBRecordWriter
 * @Description 接收reducer的输出最终写到mongodb中
 * @Author Elwyn
 * @Version 2017/8/3
 * @Copyright 上海云辰信息科技有限公司
 **/
public class MongoDBRecordWriter<V extends MongoDBWritable> extends RecordWriter<NullWritable, V> {
	private MongoCollection collection;

	public MongoDBRecordWriter() {
		super();
	}

	public MongoDBRecordWriter(TaskAttemptContext taskAttemptContext) {
		MongoUtil instance = MongoUtil.instance;
		//获取集合
		collection = instance.getCollection("test","result");

	}

	@Override
	public void write(NullWritable nullWritable, V v) throws IOException, InterruptedException {
		v.write(this.collection);
	}

	@Override
	public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

	}
}
