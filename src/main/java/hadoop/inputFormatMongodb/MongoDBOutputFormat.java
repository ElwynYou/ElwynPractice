package hadoop.inputFormatMongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
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

    /**
     * 自定义mongodb outputformat
     *
     * @param <V>
     */
    static class MongoDBRecordWriter<V extends MongoDBWritable> extends RecordWriter<NullWritable, V> {
        private MongoCollection collection;

        public MongoDBRecordWriter() {
            super();
        }

        public MongoDBRecordWriter(TaskAttemptContext taskAttemptContext) {
            //获取连接
            MongoClient mongoClient = new MongoClient("192.168.128.131", 27017);
            // 获取数据库
            MongoDatabase database = mongoClient.getDatabase("hadoop");
            //获取集合
            collection = database.getCollection("result");

        }

        @Override
        public void write(NullWritable nullWritable, V v) throws IOException, InterruptedException {
            v.write(this.collection);
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        }
    }

    @Override
    public RecordWriter<NullWritable, V> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return null;
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new FileOutputCommitter(null, taskAttemptContext);
    }
}
