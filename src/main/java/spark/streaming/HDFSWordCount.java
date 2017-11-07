package spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @Package spark.streaming
 * @Description:
 * @Author elwyn
 * @Date 2017/11/7 22:13
 * @Email elonyong@163.com
 */
public class HDFSWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WordCount");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaDStream<String> lines = javaStreamingContext.textFileStream("hdfs://hadoop-senior.ibeifeng.com:8020/wordcount_dir");

        lines.flatMap(x-> Arrays.asList(x.split(" ")).iterator())
                .mapToPair(x->new Tuple2<String, Integer>(x,1))
                .reduceByKey((x,y)->x+y).print();


        //必须调用start方法才会开始
        javaStreamingContext.start();
        //一直等待
        javaStreamingContext.awaitTermination();
        javaStreamingContext.close();
    }

}
