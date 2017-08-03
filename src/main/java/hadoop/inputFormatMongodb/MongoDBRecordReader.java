package hadoop.inputFormatMongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import mongoDB.MongoUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.bson.Document;

import java.io.IOException;

/**
 * @Name hadoop.inputFormatMongodb.MongoDBRecordReader
 * @Description  recordReader的<k,v>输出类型就是map的<k,v>输入类型
 * @Author Elwyn
 * @Version 2017/8/2
 * @Copyright 上海云辰信息科技有限公司
 **/
public class MongoDBRecordReader<V extends MongoDBWritable> extends RecordReader<LongWritable, V> {
	private MongoDBInputSplit split;
	private MongoCursor mongoCursor;
	private int index;
	private LongWritable key;
	private V value;

	public MongoDBRecordReader() {
		super();
	}

	public MongoDBRecordReader(InputSplit split, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		super();
		this.initialize(split, taskAttemptContext);
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		this.split = (MongoDBInputSplit) inputSplit;
		Configuration configuration = taskAttemptContext.getConfiguration();
		key = new LongWritable();

		//指定value的类型 也就是mapper的输入value 需要配合在执行job时set进来
		Class clz = configuration.getClass("mapreduce.mongo.split.value.class", NullMongoDBWritable.class);
		value = (V) ReflectionUtils.newInstance(clz, configuration);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (this.mongoCursor == null) {
			MongoUtil instance = MongoUtil.instance;
			//获取集合
			MongoCollection<Document> persons = instance.getCollection("test", "config");
			//通过分片规则,获取对应的数据
			mongoCursor = persons.find().skip((int) split.getStart()).limit((int) this.split.getLength()).iterator();
		}
		boolean hasNext = this.mongoCursor.hasNext();
		//设置value的值 提供给mapper的valueIn使用
		if (hasNext) {
			Document object = (Document) mongoCursor.next();
			this.key.set(this.split.getStart() + index);
			this.index++;
			this.value.readFields(object);
		}
		return hasNext;

	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return this.key;
	}

	@Override
	public V getCurrentValue() throws IOException, InterruptedException {
		return this.value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {
		this.mongoCursor.close();
	}

}
