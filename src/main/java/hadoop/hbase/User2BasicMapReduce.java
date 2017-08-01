package hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @Package hadoop.hbase
 * @Description: //todo(用一句话描述该文件做什么)
 * @Author elwyn
 * @Date 2017/7/22 0:48
 * @Email elonyong@163.com
 */
public class User2BasicMapReduce extends Configured implements Tool {



    public static class ReadUserMapper extends TableMapper<Text,Put>{
        private Text mapOutputkey =new Text();

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
          //拿到rowkey
            String rowkey= Bytes.toString(key.get());
            //设置rowkey
            mapOutputkey.set(rowkey);

            Put put = new Put(key.get());

            for (Cell cell : value.rawCells()) {
                if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
                    if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                        put.add(cell);
                    }
                    if ("age".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                        put.add(cell);
                    }
                }
            }
            context.write(mapOutputkey,put);
        }
    }

    public static class WriteBasicReducer extends TableReducer<Text,Put,ImmutableBytesWritable>{
        @Override
        protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
            for (Put put : values) {
                context.write(null,put);
            }
        }
    }
    @Override
    public int run(String[] strings) throws Exception {
        Job job=Job.getInstance(this.getConf(),this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        Scan scan=new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(
               "user",
                scan,
                ReadUserMapper.class,
                Text.class,
                Put.class,
                job
        );

        TableMapReduceUtil.initTableReducerJob(
                "basic",
                WriteBasicReducer.class,
                job

        );
        job.setNumReduceTasks(1);
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        //get configuration
        Configuration configuration= HBaseConfiguration.create();


        int status= ToolRunner.run(configuration,new User2BasicMapReduce(),args);
        System.exit(status);
    }
}
