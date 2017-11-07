package spark.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * @Package spark.streaming
 * @Description:
 * @Author elwyn
 * @Date 2017/11/7 22:49
 * @Email elonyong@163.com
 */
public class KafkaDirectWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("KafkaDirectWordCount");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(1));

        Map<String, String> map = new HashMap<>();
        //如果不用zookeeper就要设置这个参数
        map.put("metadata.broker.list", "");
        Set<String> topic = new HashSet<>();
        topic.add("topic");
        JavaPairInputDStream<String, String> directStream = KafkaUtils.createDirectStream(javaStreamingContext, String.class, String.class,
                StringDecoder.class, StringDecoder.class, map, topic);
        directStream.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterator<String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return Arrays.asList(stringStringTuple2._2.split(" ")).iterator();
            }
        }).mapToPair(x -> new Tuple2<>(x, 1)).reduceByKey((x, y) -> x + y).print();


        //必须调用start方法才会开始
        javaStreamingContext.start();
        //一直等待
        javaStreamingContext.awaitTermination();
        javaStreamingContext.close();
    }
}
