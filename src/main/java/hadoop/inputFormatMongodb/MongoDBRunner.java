package hadoop.inputFormatMongodb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

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
	 * map会接收inputformat中的recordReader的输出结果 KEYIN, VALUEIN
	 */
	static class MongoDBMapper extends Mapper<LongWritable, PersonMongoDBWriteable, IntWritable, PersonMongoDBWriteable> {
		@Override
		protected void map(LongWritable key, PersonMongoDBWriteable value, Context context) throws IOException, InterruptedException {
			if (value.getAge() == null) {
				System.out.println("过滤数据" + value.getName());
				return;
			}
			context.write(new IntWritable(value.getAge()), value);
		}
	}

	static class MongoDBReducer extends Reducer<IntWritable, PersonMongoDBWriteable, NullWritable, PersonMongoDBWriteable> {
		@Override
		protected void reduce(IntWritable key, Iterable<PersonMongoDBWriteable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (PersonMongoDBWriteable value : values) {
				sum += value.getCount();
			}
			PersonMongoDBWriteable personMongoDBWriteable = new PersonMongoDBWriteable();
			personMongoDBWriteable.setAge(key.get());
			personMongoDBWriteable.setCount(sum);
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
