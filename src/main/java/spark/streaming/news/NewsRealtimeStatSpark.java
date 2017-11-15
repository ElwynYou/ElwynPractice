package spark.streaming.news;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * @Package spark.streaming.news
 * @Description:
 * @Author elwyn
 * @Date 2017/11/13 21:52
 * @Email elonyong@163.com
 */
public class NewsRealtimeStatSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NewsRealtimeStatSpark");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(10));
        Collection<String> topicsSet = new HashSet<>();
        //kafka相关参数，必要！缺了会报错
        Map<String, Object> kafkaParams = new HashMap<>();
        String brokers = "bigdata04.nebuinfo.com:9092,bigdata05.nebuinfo.com:9092,bigdata06.nebuinfo.com:9092";
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        kafkaParams.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
        kafkaParams.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        JavaInputDStream<ConsumerRecord<String, String>> lines = KafkaUtils.createDirectStream(
                javaStreamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams)
        );

        //从所有数据中过滤出来,访问日志
        JavaDStream<ConsumerRecord<String, String>> filter = lines.filter(new Function<ConsumerRecord<String, String>, Boolean>() {
            @Override
            public Boolean call(ConsumerRecord<String, String> v1) throws Exception {
                String value = v1.value();
                String[] split = value.split(" ");
                String action = split[5];
                return "view".equals(action);
            }
        });



        //统计第一个指标:每10秒内的各个页面的pv


    }

    private static void calculatePagePv(JavaDStream<ConsumerRecord<String, String>> javaDStream){
        JavaPairDStream<Long, Long> pageIdDStream = javaDStream.mapToPair(new PairFunction<ConsumerRecord<String, String>, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(ConsumerRecord<String, String> consumerRecord) throws Exception {
                String value = consumerRecord.value();
                String[] split = value.split(" ");
                Long pageId = Long.valueOf(split[3]);

                return new Tuple2<>(pageId, 1L);
            }
        });
        pageIdDStream.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1+v2;
            }
        }).print();
        //计算出每10秒之后持久化;
    }
}
