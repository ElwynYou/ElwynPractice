package hadoop.shuffle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Package hadoop.shuffle
 * @Description: //todo(用一句话描述该文件做什么)
 * @Author elwyn
 * @Date 2017/8/2 21:45
 * @Email elonyong@163.com
 */
public class Runner {
    static class DemoMapper extends Mapper<Object, Text, IntPair, IntWritable> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] strs = line.split("\\s");
            if (strs.length == 2) {
                int first = Integer.valueOf(strs[0]);
                int second = Integer.valueOf(strs[1]);
                context.write(new IntPair(first, second), new IntWritable(second));
            } else {
                System.err.println("数据异常：" + line);
            }
        }
    }

    static class DemoReducer extends Reducer<IntPair, IntWritable, IntWritable, Text> {
        @Override
        protected void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Integer preKey = key.getFirst();
            StringBuffer stringBuffer = new StringBuffer();
            for (IntWritable value : values) {
                int curKey = key.getFirst();
                if (curKey == preKey) {
                    //是同一个key，但是value是不一样的或者value是排序好的
                    stringBuffer.append(value.get()).append(",");
                } else {
                    //表示是一个新的key，先输出旧的key对应新的value信息，然后修改key值和stringbuffuer的值
                    context.write(new IntWritable(preKey), new Text(stringBuffer.toString()));
                    preKey = curKey;
                    stringBuffer = new StringBuffer();
                    stringBuffer.append(value.get()).append(",");
                }
            }
            context.write(new IntWritable(preKey), new Text(stringBuffer.toString()));

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop-master.nebuinfo.com:8020");
        System.setProperty("HADOOP_USER_NAME", "yunchen");

        Job job = Job.getInstance(configuration, "demo-job");

        job.setJarByClass(Runner.class);
        job.setMapperClass(DemoMapper.class);
        job.setReducerClass(DemoReducer.class);

        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(IntPairGrouping.class);
        //设置partitioner。要求reducer个数必须大于1
        job.setPartitionerClass(IntPairPartitioner.class);
        job.setNumReduceTasks(2);

        FileInputFormat.setInputPaths(job, "/user/yunchen/mapreduce/shuffle/input");
        FileOutputFormat.setOutputPath(job, new Path("/user/yunchen/mapreduce/shuffle/output"));
        job.waitForCompletion(true);
    }
}
