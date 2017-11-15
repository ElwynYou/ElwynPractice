package spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

/**
 * @Package spark.streaming
 * @Description: 滑动窗口热点搜索词统计
 * @Author elwyn
 * @Date 2017/11/8 21:52
 * @Email elonyong@163.com
 */
public class WindowHotWord  {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WindowHotWord");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaReceiverInputDStream<String> localhost = javaStreamingContext.socketTextStream("localhost", 9999);
        JavaPairDStream<String, Integer> stringObjectJavaPairDStream = localhost.map(new Function<String, String>() {
            @Override
            public String call(String searchLog) throws Exception {
                return searchLog.split(" ")[1];
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
            //10秒间隔到了以后会将之前60秒的rdd聚合,因为一个batch间隔是5秒,所以之前的
            //60秒就有12个rdd,给聚合起来,统一执行reduceByKey操作
            //所以这里的reducebykey是针对window操作的
        }).reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
            //窗口的长度,60秒       滑动间隔10秒
        }, Durations.seconds(60), Durations.seconds(10))
                //通过窗口函数,每隔10秒收集之前60秒的单次统计
                //执行transform操作,因为,一个窗口,就是一个60秒钟的数据,会变成一个RDD,
                //然后对这个rdd根据搜索词排序,获取前三名的热点搜索词
                .transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
                    @Override
                    public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> v1) throws Exception {
                        //反转key 和value
                        JavaPairRDD<String, Integer> integerStringJavaPairRDD = v1.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                            @Override
                            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                                return new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2._1);
                            }
                            //降序排序
                        }).sortByKey(false)
                                //再次反转
                                .mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                                    @Override
                                    public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                                        return new Tuple2<>(integerStringTuple2._2, integerStringTuple2._1);
                                    }
                                });
                        //拿到前3哥
                        List<Tuple2<String, Integer>> take = integerStringJavaPairRDD.take(3);
                        for (Tuple2<String, Integer> stringIntegerTuple2 : take) {
                            System.out.println(stringIntegerTuple2);
                        }
                        return integerStringJavaPairRDD;
                    }
                });
        stringObjectJavaPairDStream.print();
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
        javaStreamingContext.close();
    }
}
