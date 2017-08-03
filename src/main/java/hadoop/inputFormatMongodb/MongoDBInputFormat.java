package hadoop.inputFormatMongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import mongoDB.MongoUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Package hadoop.inputFormatMongodb
 * @Description: 自定义mongoDb Inputformat
 * @Author elwyn
 * @Date 2017/8/1 22:47
 * @Email elonyong@163.com
 */
public class MongoDBInputFormat<V extends MongoDBWritable> extends InputFormat<LongWritable, V> {
	/**
	 * 先通过此方法获取分片信息
	 *
	 * @param jobContext
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
		MongoUtil mongoUtil = MongoUtil.instance;

		MongoCollection<Document> persons = mongoUtil.getCollection("test", "config");
		//定义分片的规则
		//每两条数据一个mapper
		int chunkSize = 2;
		long size = persons.count();//获取mongodb对应collection的数据条数
		long chunk = size / chunkSize;//计算mapper个数
		List<InputSplit> list = new ArrayList<>();
		for (int i = 0; i < chunk; i++) {
			if (i + 1 == chunk) {
				list.add(new MongoDBInputSplit(i * chunkSize, size));
			} else {
				list.add(new MongoDBInputSplit(i * chunkSize, i * chunkSize + chunkSize));
			}
		}
		return list;
	}

	/**
	 * 获取到分片以后调用此方法获取具体的分片进行处理
	 *
	 * @param inputSplit
	 * @param taskAttemptContext
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public RecordReader<LongWritable, V> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		return new MongoDBRecordReader<>(inputSplit, taskAttemptContext);
	}

}
