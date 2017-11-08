package spark.streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/**
 * @Name spark.streaming.Kafka
 * @Description
 * @Author Elwyn
 * @Version 2017/11/7
 * @Copyright 上海云辰信息科技有限公司
 **/
public class Kafka {
	public static void main(String[] args) throws InterruptedException {
		String brokers = "bigdata04.nebuinfo.com:9092,bigdata05.nebuinfo.com:9092,bigdata06.nebuinfo.com:9092";
		String topics = "topicName";
		SparkConf conf = new SparkConf().setAppName("streaming word count").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));

		Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		//kafka相关参数，必要！缺了会报错
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
		kafkaParams.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
		kafkaParams.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

		//Topic分区
			/*Map<TopicPartition, Long> offsets = new HashMap<>();
			offsets.put(new TopicPartition("topicName", 0), 2L);*/
		//通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
		JavaInputDStream<ConsumerRecord<Object, Object>> lines = KafkaUtils.createDirectStream(
				ssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(topicsSet, kafkaParams)
		);
		//这里就跟之前的demo一样了，只是需要注意这边的lines里的参数本身是个ConsumerRecord对象
		JavaPairDStream<String, Integer> counts =
				lines.flatMap(x -> Arrays.asList(x.value().toString().split(" ")).iterator())
						.mapToPair(x -> new Tuple2<>(x, 1))
						.reduceByKey((x, y) -> x + y);
		counts.print();
//  可以打印所有信息，看下ConsumerRecord的结构
//      lines.foreachRDD(rdd -> {
//          rdd.foreach(x -> {
//            System.out.println(x);
//          });
//        });
		ssc.start();
		ssc.awaitTermination();
		ssc.close();
	}

}
