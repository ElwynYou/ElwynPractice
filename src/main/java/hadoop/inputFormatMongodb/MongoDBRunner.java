package hadoop.inputFormatMongodb;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.client.MongoCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.Document;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Package hadoop.inputFormatMongodb
 * @Description: 操作mongodb的主类
 * @Author elwyn
 * @Date 2017/8/1 23:53
 * @Email elonyong@163.com
 */
public class MongoDBRunner {
    /**
     * Mongodb转换到hadoop的bean
     */
    static class PersonMongoDBWriteable implements MongoDBWritable {

        private String name;
        private Integer age;
        private String sex = "";
        private int count = 0;

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(this.name);
            dataOutput.writeUTF(this.sex);
            if (this.age == null) {
                dataOutput.writeBoolean(false);
            } else {
                dataOutput.writeBoolean(true);
                dataOutput.writeInt(this.age);
            }
            dataOutput.writeInt(this.count);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.name = dataInput.readUTF();
            this.sex = dataInput.readUTF();
            if (dataInput.readBoolean()) {
                this.age = dataInput.readInt();
            } else {
                this.age = null;
            }
            this.count = dataInput.readInt();
        }

        @Override
        public void readFields(Document object) {
            this.name = object.getString("name");
            if (object.get("age") != null) {
                this.age = object.getInteger("age");
            } else {
                this.age = null;
            }
        }

        @Override
        public void write(MongoCollection mongoCollection) {
            DBObject dbObject = BasicDBObjectBuilder.start().add("age", this.age)
                    .add("count", this.count).get();
            mongoCollection.insertOne(dbObject);
        }
    }

    static class MongoDBMapper extends Mapper<LongWritable, PersonMongoDBWriteable, IntWritable, PersonMongoDBWriteable> {
        @Override
        protected void map(LongWritable key, PersonMongoDBWriteable value, Context context) throws IOException, InterruptedException {
            if (value.age == null) {
                System.out.println("过滤数据" + value.name);
                return;
            }
            context.write(new IntWritable(value.age), value);
        }
    }

    static class MongoDBReducer extends Reducer<IntWritable, PersonMongoDBWriteable, NullWritable, PersonMongoDBWriteable> {
        @Override
        protected void reduce(IntWritable key, Iterable<PersonMongoDBWriteable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (PersonMongoDBWriteable value : values) {
                sum += value.count;
            }
            PersonMongoDBWriteable personMongoDBWriteable = new PersonMongoDBWriteable();
            personMongoDBWriteable.age = key.get();
            personMongoDBWriteable.count = sum;
            context.write(NullWritable.get(), personMongoDBWriteable);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        //设置inputformat的value类
        configuration.setClass("mapreduce.mongo.split.value.class", PersonMongoDBWriteable.class, MongoDBWritable.class);
        Job job = Job.getInstance(configuration, "自定义input/output");
        job.setJarByClass(MongoDBRunner.class);
        job.setMapperClass(MongoDBMapper.class);
        job.setReducerClass(MongoDBReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PersonMongoDBWriteable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(PersonMongoDBWriteable.class);

        job.setInputFormatClass(MongoDBInputFormat.class);
        job.setOutputFormatClass(MongoDBOutputFormat.class);

        job.waitForCompletion(true);
    }
}
