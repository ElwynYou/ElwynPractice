package spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @Package spark.streaming
 * @Description:
 * @Author elwyn
 * @Date 2017/11/7 22:36
 * @Email elonyong@163.com
 */
public class KafkaReceiverWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WordCount");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(1));
        Map<String, Integer> stringIntegerMap = new HashMap<>();
        stringIntegerMap.put("WordCount", 1);
       /* JavaPairReceiverInputDStream<String, String> aDefault = KafkaUtils.createStream(javaStreamingContext, "", "default", stringIntegerMap);
        JavaDStream<String> stringJavaDStream = aDefault.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterator<String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return Arrays.asList(stringStringTuple2._2.split(" ")).iterator();
            }
        });

        stringJavaDStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        }).reduceByKey((x,y)->x+y).print();*/


        //必须调用start方法才会开始
        javaStreamingContext.start();
        //一直等待
        javaStreamingContext.awaitTermination();
        javaStreamingContext.close();
    }
}
