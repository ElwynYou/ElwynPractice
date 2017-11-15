package spark.demo;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import spark.ScanEndingImprove;
import spark.streaming.ConnectionPool;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * @Name spark.demo.Kafka2HbaseOracle
 * @Description
 * @Author Elwyn
 * @Version 2017/11/14
 * @Copyright 上海云辰信息科技有限公司
 **/
public class Kafka2HBaseOracle {
	public static void main(String[] args) {
		final String brokers = "bigdata04.nebuinfo.com:9092,bigdata05.nebuinfo.com:9092,bigdata06.nebuinfo.com:9092";
		final String topics = "scanEnding";
		String[] topicArry = topics.split(",");

		SparkConf sparkConf = new SparkConf().setAppName("Kafka2HBaseOracle").setMaster("local");
		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

		//kafka相关参数,
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
		kafkaParams.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
		kafkaParams.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

		//利用 KafkaUtils.createDirectStream接收kafka消息;
		JavaInputDStream<ConsumerRecord<String, String>> lines = KafkaUtils.createDirectStream(
				javaStreamingContext, LocationStrategies.PreferBrokers(),
				ConsumerStrategies.Subscribe(Arrays.asList(topicArry), kafkaParams));
		//通过map算子转换为bean并且做持久化便于多次调用
		JavaDStream<ScanEndingImprove> scanEndingImproveJavaDStream = lines.map(new Function<ConsumerRecord<String, String>, ScanEndingImprove>() {
			@Override
			public ScanEndingImprove call(ConsumerRecord<String, String> v1) throws Exception {
				return JSONObject.parseObject(v1.value(), ScanEndingImprove.class);
			}
		}).cache();

//===================================持久化到HBase=============================================================

		scanEndingImproveJavaDStream.foreachRDD(new VoidFunction<JavaRDD<ScanEndingImprove>>() {
			@Override
			public void call(JavaRDD<ScanEndingImprove> scanEndingImproveJavaRDD) throws Exception {
				if (scanEndingImproveJavaRDD.isEmpty()) {
					return;
				}
				//不能用spark的javaSparkContext.hadoopConfiguration()获取conf
				Configuration conf = HBaseConfiguration.create();
				conf.set("hbase.zookeeper.quorum", "bigdata01.nebuinfo.com");
				conf.set(TableOutputFormat.OUTPUT_TABLE, "scanEndingImprove");
				//必须要利用conf创建Job对象,并设置输出格式,否则会出异常
				//Exception in thread "main" org.apache.hadoop.mapred.InvalidJobConfException: Output directory not set.
				Job newAPIJobConfiguration = null;
				try {
					newAPIJobConfiguration = Job.getInstance(conf);
					newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
				} catch (IOException e) {
					e.printStackTrace();
				}
				if (newAPIJobConfiguration != null) {
					scanEndingImproveJavaRDD.mapToPair(new PairFunction<ScanEndingImprove, ImmutableBytesWritable, Put>() {
						@Override
						public Tuple2<ImmutableBytesWritable, Put> call(ScanEndingImprove scanEndingImprove) throws Exception {
							//传入rowkey格式:imsi_areaCode
							String rowKey = scanEndingImprove.getImsi() + "_" + scanEndingImprove.getAreaCode();
							String family = "info";
							//构建put对象
							Put put = generatePut(scanEndingImprove, rowKey, family);
							return new Tuple2<>(new ImmutableBytesWritable(), put);
						}
						//持久化到HBase
					}).saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());
				}
			}
		});
//===================================持久化到Oracle============================================================
		//按照imsi,实时统计并累加每个imsi的捕获次数
		javaStreamingContext.checkpoint("hdfs://bigdata02.nebuinfo.com:8020/scan_checkPoint");
		scanEndingImproveJavaDStream.mapToPair(new PairFunction<ScanEndingImprove, ScanEndingImprove, Long>() {
			@Override
			public Tuple2<ScanEndingImprove, Long> call(ScanEndingImprove scanEndingImprove) throws
					Exception {
				//用scanEndingImprove对象作为key,方便后面的保存操作,
				//如果用对象做key必须重写hashcode.
				// 这里重写了ScanEndingImprove的hashcode保证同一个imsi会被累加
				return new Tuple2<>(scanEndingImprove, 1L);
			}
			//利用updateStateByKey算子累加每个imsi的个数
		}).updateStateByKey(new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
			@Override
			public Optional<Long> call(List<Long> counts, Optional<Long> state) throws Exception {
				//首先定义一个全局的计数
				Long newCount = 0L;
				//其次,判断,state是否存在,如果不存在,说明是一个key第一次出现
				//如果存在,说明这个key之前已经统计过全局的次数了
				if (state.isPresent()) {
					newCount = state.get();
				}
				//接着,将本次新出现的值,都累加到newvalue上去,就是一个key目前的全局的统计
				for (Long count : counts) {
					newCount += count;
				}
				return Optional.of(newCount);
			}
		}).foreachRDD(new VoidFunction<JavaPairRDD<ScanEndingImprove, Long>>() {
			@Override
			public void call(JavaPairRDD<ScanEndingImprove, Long> scanEndingImproveLongJavaPairRDD) throws Exception {
				//这里用foreachPartition而不用foreach可以提高性能
				scanEndingImproveLongJavaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<ScanEndingImprove, Long>>>() {
					@Override
					public void call(Iterator<Tuple2<ScanEndingImprove, Long>> tuple2Iterator) throws Exception {
						Tuple2<ScanEndingImprove, Long> tuple2 = null;
						//给每个partition获取一个链接
						Connection connection = ConnectionPool.getConnection();
						//遍历partition中的数据,使用一个链接,插入数据库
						while (tuple2Iterator.hasNext()) {
							tuple2 = tuple2Iterator.next();
							String imsi = tuple2._1.getImsi();
							Long nums = tuple2._2;
							System.out.println(imsi + nums);
							String sql = "MERGE INTO imsi_count a " +
									"USING (SELECT '" + imsi + "' AS imsi," + nums + " AS nums FROM dual) b " +
									"ON ( a.imsi=b.imsi) " +
									"WHEN MATCHED THEN " +
									"    UPDATE SET a.nums = b.nums " +
									"WHEN NOT MATCHED THEN  " +
									"    INSERT (imsi,nums) VALUES(b.imsi,b.nums)";
							Statement statement = connection.createStatement();
							try {
								statement.execute(sql);
							} catch (SQLException e) {
								e.printStackTrace();
							}
						}
						ConnectionPool.returnConnection(connection);
					}
				});
			}
		});

		//必须调用start方法才会开始
		javaStreamingContext.start();
		//一直等待
		try {
			javaStreamingContext.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		javaStreamingContext.close();
	}

	//根据属性名作为column名称生成put
	private static Put generatePut(Object o, String row, String family) throws IllegalAccessException {
		if (row == null) {
			return null;
		}
		Put put = new Put(Bytes.toBytes(row));
		Field[] declaredFields = o.getClass().getDeclaredFields();
		for (Field declaredField : declaredFields) {
			String name = declaredField.getName();
			if (name.equals("serialVersionUID")) {
				continue;
			}
			declaredField.setAccessible(true);
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(name), Bytes.toBytes(String.valueOf(declaredField.get(o))));
		}
		return put;
	}
}
