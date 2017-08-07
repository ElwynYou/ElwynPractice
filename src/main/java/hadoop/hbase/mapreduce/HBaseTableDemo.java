package hadoop.hbase.mapreduce;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * @Package hadoop.hbase.mapreduce
 * @Description: //todo(用一句话描述该文件做什么)
 * @Author elwyn
 * @Date 2017/8/6 2:52
 * @Email elonyong@163.com
 */
public class HBaseTableDemo {
    static Map<String, String> transforContent(String content) {
        Map<String, String> map = new HashMap<>();
        int i = 0;
        String key = "";
        StringTokenizer stringTokenizer = new StringTokenizer(content, "({|}|\"|:|,)");
        while (stringTokenizer.hasMoreTokens()) {
            if (++i % 2 == 0) {
                map.put(key, stringTokenizer.nextToken());
            } else {
                key = stringTokenizer.nextToken();
            }
        }
        return map;
    }

    static class DemoMapper extends TableMapper<Text, ProductModel> {
        private Text outputKey = new Text();
        private ProductModel outputvalue = new ProductModel();

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String content = Bytes.toString(value.getValue(Bytes.toBytes("f"), Bytes.toBytes("content")));
            if (content == null) {
                System.err.println("数据格式错误");
                return;
            }
            Map<String, String> stringStringMap = transforContent(content);
            if (stringStringMap.containsKey("p_id")) {
                outputKey.set(stringStringMap.get("p_id"));
            } else {
                System.err.println("数据格式错");
                return;
            }

            if (stringStringMap.containsKey("p_name") && stringStringMap.containsKey("price")) {
                outputvalue.setId(outputKey.toString());
                outputvalue.setName(stringStringMap.get("p_name"));
                outputvalue.setPrice(stringStringMap.get("price"));
            } else {
                System.err.println("数据格式错");
                return;
            }
            context.write(outputKey, outputvalue);
        }


    }

    static class DemoReducer extends TableReducer<Text, ProductModel, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<ProductModel> values, Context context) throws IOException, InterruptedException {
            for (ProductModel value : values) {
                ImmutableBytesWritable outputkey = new ImmutableBytesWritable(Bytes.toBytes(key.toString()));
                Put put = new Put(Bytes.toBytes(key.toString()));
                put.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes(value.getId()));
                put.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes(value.getName()));
                put.add(Bytes.toBytes("f"), Bytes.toBytes("price"), Bytes.toBytes(value.getPrice()));
                context.write(outputkey, put);
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = initLocalHbaseMapReduceJobConfig();
        int l = job.waitForCompletion(true) ? 0 : 1;
        System.out.println("执行" + l);
    }

    /**
     * 本地运行模式
     * @return
     * @throws IOException
     */
    static Job initLocalHbaseMapReduceJobConfig() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
      //  configuration.set("fs.defaultFs", "hdfs://hadoop-senior.ibeifeng.com");
        configuration.set("hbase.zookeeper.quorum", "hadoop-senior.ibeifeng.com");
        System.setProperty("hadoop.home.dir", "E:\\hadoop-2.5.0");
        String home = System.getProperty("hadoop.home.dir");
        System.out.println(home);
        Job job = Job.getInstance(configuration, "demo");
        job.setJarByClass(HBaseTableDemo.class);
        //设置mapper相关，mapper从hbase输入
        //本地环境最后一个参数必须味false
        TableMapReduceUtil.initTableMapperJob("data1", new Scan(), DemoMapper.class, Text.class, ProductModel.class, job, false);

        //设置reducer相关，reducer往hbase输出
        //本地环境，而且fs。defaultFs为集群需要设置最后参数为false
        TableMapReduceUtil.initTableReducerJob("online_product", DemoReducer.class, job, null, null, null, null, false);
        return job;
    }
    static Job initLocalHbaseMapReduceJobConfig2()throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        //如果注释掉就说明是在本地运行
        //configuration.set("fs.defaultFs", "hdfs://hadoop-senior.ibeifeng.com:8020");
        configuration.set("hbase.zookeeper.quorum", "hadoop-senior.ibeifeng.com");

        Job job = Job.getInstance(configuration, "demo");
        job.setJarByClass(HBaseTableDemo.class);
        //设置mapper相关，mapper从hbase输入
        //本地环境不用那么多参数的方法
        TableMapReduceUtil.initTableMapperJob("data1", new Scan(), DemoMapper.class, Text.class, ProductModel.class, job );

        //设置reducer相关，reducer往hbase输出
        //本地环境不用那么多参数的方法
        TableMapReduceUtil.initTableReducerJob("online_product", DemoReducer.class, job);
        return job;
    }


}
