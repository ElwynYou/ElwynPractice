package spark.streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @Name spark.streaming.Kafka2Oracle
 * @Description
 * @Author Elwyn
 * @Version 2017/11/8
 * @Copyright 上海云辰信息科技有限公司
 **/
public class Kafka2Oracle {
	public static void main(String[] args) throws InterruptedException {
		String brokers = "bigdata04.nebuinfo.com:9092,bigdata05.nebuinfo.com:9092,bigdata06.nebuinfo.com:9092";
		String topics = "topicName";
		SparkConf conf = new SparkConf().setAppName("Kafka2Oracle");
		JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(1));

		Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
		kafkaParams.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "3000");
	/*	kafkaParams.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
		kafkaParams.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "25000");
		kafkaParams.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "20000");
		kafkaParams.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "20000");*/
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

		//Topic分区
			/*Map<TopicPartition, Long> offsets = new HashMap<>();
			offsets.put(new TopicPartition("topicName", 0), 2L);*/
		//通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
		JavaInputDStream<ConsumerRecord<Integer, String>> lines = KafkaUtils.createDirectStream(
				ssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.Subscribe(topicsSet, kafkaParams)
		);
		//这里就跟之前的demo一样了，只是需要注意这边的lines里的参数本身是个ConsumerRecord对象

		lines.map(new Function<ConsumerRecord<Integer, String>, ServiceInfo>() {
			@Override
			public ServiceInfo call(ConsumerRecord<Integer, String> integerStringConsumerRecord) throws Exception {
				String[] split = integerStringConsumerRecord.value().split(",");
				ServiceInfo serviceInfo = new ServiceInfo();
				serviceInfo.setServiceCode(split[0]);
				serviceInfo.setServiceName(split[1]);
				serviceInfo.setServiceType(split[2]);
				return serviceInfo;
			}
		}).print();/*.foreachRDD(new VoidFunction<JavaRDD<ServiceInfo>>() {
			@Override
			public void call(JavaRDD<ServiceInfo> serviceInfoJavaRDD) throws Exception {
				serviceInfoJavaRDD.foreachPartition(new VoidFunction<Iterator<ServiceInfo>>() {
					@Override
					public void call(Iterator<ServiceInfo> serviceInfoIterator) throws Exception {
						*//*PreparedStatement preparedStatement = null;
						Connection connection = ConnectionPool.getConnection();*//*

							while (serviceInfoIterator.hasNext()) {
								ServiceInfo next = serviceInfoIterator.next();
								*//*String sql = "INSERT INTO service_info (service_code,service_name,service_type) VALUES(?,?,?)";
								System.out.println(connection);
								System.out.println("++++++++++++++++++++==============================");
								connection.setAutoCommit(false);
								preparedStatement = connection.prepareStatement(sql);
								preparedStatement.setString(1, next.getServiceCode());
								preparedStatement.setString(2, next.getServiceName());
								preparedStatement.setString(3, next.getServiceType());
								preparedStatement.addBatch();*//*
								System.out.println(next);
							}
							*//*if (preparedStatement != null) {
								preparedStatement.executeBatch();
							}
							if (connection != null) {
								connection.commit();
								connection.setAutoCommit(true);
							}
						} catch (SQLException e) {
							e.printStackTrace();
						} finally {
							if (connection != null) {
								ConnectionPool.returnConnection(connection);
							}
						}*//*


					}
				});
			}
		});*/


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

	private static class ServiceInfo implements Serializable {
		private String serviceCode;
		private String serviceName;
		private String serviceType;

		public String getServiceCode() {
			return serviceCode;
		}

		public void setServiceCode(String serviceCode) {
			this.serviceCode = serviceCode;
		}

		public String getServiceName() {
			return serviceName;
		}

		public void setServiceName(String serviceName) {
			this.serviceName = serviceName;
		}

		public String getServiceType() {
			return serviceType;
		}

		public void setServiceType(String serviceType) {
			this.serviceType = serviceType;
		}

		@Override
		public String toString() {
			return "ServiceInfo{" +
					"serviceCode='" + serviceCode + '\'' +
					", serviceName='" + serviceName + '\'' +
					", serviceType='" + serviceType + '\'' +
					'}';
		}
	}
}
