package spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @Package spark.streaming
 * @Description:
 * @Author elwyn
 * @Date 2017/11/7 23:20
 * @Email elonyong@163.com
 */
public class UpdateStateByKey {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("UpdateStateByKey");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        //开启checkpoint 设置一个hdfs目录即可
        javaStreamingContext.checkpoint("hdfs://bigdata02.nebuinfo.com:8020/checkpoint_dir");
        JavaReceiverInputDStream<String> spark1 = javaStreamingContext.socketTextStream("bigdata05.nebuinfo.com", 9999);
        JavaPairDStream<String, Integer> pairDStream = spark1.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).mapToPair(x -> new Tuple2<>(x, 1));
        //基于缓存可以统计全局的计数累加
        //spark 维护一份全局的计数统计
        //每次batch计算的时候都会调用这个函数
        //第一个参数values相当与是这个batch,key的新值,可能有多个
        //第二个参数,就是指这个key之前的状态,state,其中泛型的类型是自己定义的
        JavaPairDStream<String, Integer> updateStateByKey = pairDStream.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                //首先定义一个全局的单词计数
                Integer newValue = 0;
                //其次,判断,state是否存在,如果不存在,说明是一个key第一次出现
                //如果存在,说明zhegkey之前已经统计过全局的次数了
                if (state.isPresent()) {
                    newValue = state.get();
                }
                //接着,将本次新出现的值,都累加到newvalue上去,就是一个key目前的全局的统计
                for (Integer value : values) {
                    newValue += value;
                }
                return Optional.of(newValue);
            }
        });
        //updateStateByKey返回的javaPairDStream,其实就是代表了每个key的全局计数
        updateStateByKey.print();
        //必须调用start方法才会开始
        javaStreamingContext.start();
        //一直等待
        javaStreamingContext.awaitTermination();
        javaStreamingContext.close();
    }
}
