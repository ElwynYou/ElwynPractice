package spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @Package spark.streaming
 * @Description: 持久化机制的实时wordcount
 * @Author elwyn
 * @Date 2017/11/7 23:20
 * @Email elonyong@163.com
 */
public class PersistWordCount {
    public static void main(String[] args) throws InterruptedException {
        System.setProperty("HADOOP_USER_NAME", "beifeng");
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WordCount");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        //开启checkpoint 设置一个hdfs目录即可
        javaStreamingContext.checkpoint("hdfs://hadoop-senior.ibeifeng.com:8020/wordcount_checkpoint");
        JavaReceiverInputDStream<String> spark1 = javaStreamingContext.socketTextStream("hadoop-senior.ibeifeng.com", 9999);
        JavaPairDStream<String, Integer> pairDStream = spark1.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).mapToPair(x -> new Tuple2<>(x, 1));

        JavaPairDStream<String, Integer> wordcount = pairDStream.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                Integer newValue = 0;
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
        // 每次得到当前所有单词的统计次数之后，将其写入mysql存储，进行持久化，以便于后续的J2EE应用程序
        wordcount.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> wordcountRdd) throws Exception {
                //调用rdd的foreachpartion()方法
                wordcountRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {
                        //给每个partition获取一个链接
                        Connection connection = ConnectionPool.getConnection();
                        //遍历partition中的数据,使用一个链接,插入数据库
                        Tuple2<String, Integer> next =null;
                        while (tuple2Iterator.hasNext()){
                            next = tuple2Iterator.next();
                            String sql = "insert into wordcount(word,count) " +
                                    "values('" + next._1 + "'," + next._2 + ")";
                            Statement statement = connection.createStatement();
                            statement.execute(sql);
                        }
                        ConnectionPool.returnConnection(connection);
                    }
                });
            }
        });
        //必须调用start方法才会开始
        javaStreamingContext.start();
        //一直等待
        javaStreamingContext.awaitTermination();
        javaStreamingContext.close();
    }
}
