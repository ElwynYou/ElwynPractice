package hadoop.hbase;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Package hadoop.hbase
 * @Description: //todo(用一句话描述该文件做什么)
 * @Author elwyn
 * @Date 2017/7/22 14:38
 * @Email elonyong@163.com
 */
public class HFileInput extends Configured implements Tool {

    public static class ReadFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] split = line.split(",");
            ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable();
            immutableBytesWritable.set(Bytes.toBytes(split[0]));
            Put put = new Put(Bytes.toBytes(split[0]));
            put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(split[1]));
            put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(split[2]));
            put.add(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes(split[3]));
            put.add(Bytes.toBytes("info"), Bytes.toBytes("address"), Bytes.toBytes(split[4]));
            put.add(Bytes.toBytes("info"), Bytes.toBytes("tel"), Bytes.toBytes(split[5]));

            context.write(immutableBytesWritable, put);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        job.setJarByClass(HFileInput.class);
        job.setMapperClass(ReadFileMapper.class);
        FileInputFormat.addInputPaths(job, args[0]);//输入路径

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setReducerClass(PutSortReducer.class);


        FileSystem fs = FileSystem.get(this.getConf());
        Path output = new Path(args[1]);
        if (fs.exists(output)) {
            fs.delete(output, true);//如果输出路径存在，就将其删除
        }
        FileOutputFormat.setOutputPath(job, output);
        HTable hTable = new HTable(getConf(), args[2]);
        HFileOutputFormat2.configureIncrementalLoad(job, hTable);
        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        // int status = new WordCountMapReduce().run(args);
        int status = ToolRunner.run(HBaseConfiguration.create(), new HFileInput(), args);

        System.exit(status);
    }
}
