package hadoop.inputFormatMongodb;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.bson.Document;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Package hadoop.inputFormatMongodb
 * @Description: //自定义mongoDb Inputformat
 * @Author elwyn
 * @Date 2017/8/1 22:47
 * @Email elonyong@163.com
 */
public class MongoDBInputFormat<V extends MongoDBWritable> extends InputFormat<LongWritable, V> {
    /**
     * mongoDb 自定义inputsplit
     */
    static class MongoDBInputSplit extends InputSplit implements Writable {
        private long start;//起始位置,包含
        private long end;//终止位置,不包含

        public MongoDBInputSplit() {
            super();
        }

        public MongoDBInputSplit(long start, long end) {
            super();
            this.start = start;
            this.end = end;
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return end - start;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(this.start);
            dataOutput.writeLong(this.end);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.start = dataInput.readLong();
            this.end = dataInput.readLong();
        }
    }

    /**
     * 获取分片信息
     *
     * @param jobContext
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        //获取连接
        MongoClient mongoClient = new MongoClient("192.168.128.131", 27017);
        // 获取数据库
        MongoDatabase database = mongoClient.getDatabase("hadoop");
        //获取集合
        MongoCollection<Document> persons = database.getCollection("persons");
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
        mongoClient.close();
        return list;
    }

    /**
     * 获取具体的reader类
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

    static class NullMongoDBWritable implements MongoDBWritable {
        @Override
        public void readFields(Document object) {

        }

        @Override
        public void write(MongoCollection mongoCollection) {

        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {

        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {

        }
    }

    /**
     * 自定义mongodbreader
     *
     * @param <V>
     */
    static class MongoDBRecordReader<V extends MongoDBWritable> extends RecordReader<LongWritable, V> {
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
            Class clz = configuration.getClass("mapreduce.mongo.split.value.class", NullMongoDBWritable.class);
            value = (V) ReflectionUtils.newInstance(clz, configuration);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (this.mongoCursor == null) {

                //获取连接
                MongoClient mongoClient = new MongoClient("192.168.128.131", 27017);
                // 获取数据库
                MongoDatabase database = mongoClient.getDatabase("hadoop");
                //获取集合
                MongoCollection<Document> persons = database.getCollection("persons");
                mongoCursor = persons.find().skip((int) this.split.start).limit((int) this.split.getLength()).iterator();
            }
            boolean hasNext = this.mongoCursor.hasNext();
            if (hasNext) {
                Document object = (Document) mongoCursor.next();
                this.key.set(this.split.start + index);
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
}
