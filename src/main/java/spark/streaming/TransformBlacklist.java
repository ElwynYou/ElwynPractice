package spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @Name spark.streaming.TransformBlacklist
 * @Description 实时黑名单过滤
 * @Author Elwyn
 * @Version 2017/11/8
 * @Copyright 上海云辰信息科技有限公司
 **/
public class TransformBlacklist {
	public static void main(String[] args) throws InterruptedException {
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformBlacklist");
		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

		List<Tuple2<String, Boolean>> blacklist = new ArrayList<>();
		blacklist.add(new Tuple2<>("tom", true));
		JavaPairRDD<String, Boolean> blcaklistRDD = javaStreamingContext.sparkContext().parallelizePairs(blacklist);


		//用户点击的实时计费
		//但是要过滤刷广告的人,所以要黑名单
		JavaReceiverInputDStream<String> adsclickLog = javaStreamingContext.socketTextStream("192.168.60.106", 9999);
		JavaPairDStream<String, String> userAdsClickLogDStream = adsclickLog.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				System.out.println(s);
				return new Tuple2<>(s.split(" ")[1], s);
			}
		});
		JavaDStream<String> transform = userAdsClickLogDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
			@Override
			public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRdd) throws Exception {
				JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinRdd = userAdsClickLogRdd.leftOuterJoin(blcaklistRDD);
				JavaRDD<String> map = joinRdd.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
					@Override
					public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple2) throws Exception {
						//这里的tuple就是每个用户对应的访问日志,和在黑名单中的状态
						if (tuple2._2._2.isPresent() && tuple2._2._2.get()) {
							return false;
						}
						return true;
					}
					//此时,filterrdd中就只剩下没有被黑名单过滤的用户了
				}).map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
					@Override
					public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple2) throws Exception {
						return tuple2._2._1;
					}
				});
				return map;
			}
		});
		transform.print();
		javaStreamingContext.start();
		javaStreamingContext.awaitTermination();
		javaStreamingContext.close();
	}
}
