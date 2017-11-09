package spark.function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import spark.ServiceInfo;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @Name spark.function.Aggregate
 * @Description
 * 将初始值和第一个分区中的第一个元素传递给seq函数进行计算，
 * 然后将计算结果和第二个元素传递给seq函数，直到计算到最后一个值。
 * 第二个分区中也是同理操作。最后将初始值、所有分区的结果经过combine函数进行计算
 * （先将前两个结果进行计算，将返回结果和下一个结果传给combine函数，以此类推），并返回最终结果。
 * @Author Elwyn
 * @Version 2017/11/9
 * @Copyright 上海云辰信息科技有限公司
 **/
public class Aggregate {
	public static void main(String[] args) {
		List<Tuple2<String, Integer>> tuple2s = Arrays.asList(
				new Tuple2<>("A", 1),
				new Tuple2<>("A", 2),
				new Tuple2<>("B", 3),
				new Tuple2<>("B", 4),
				new Tuple2<>("C", 7));
		SparkConf sparkConf = new SparkConf().setAppName("Fold").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<Tuple2<String, Integer>> parallelize = javaSparkContext.parallelize(tuple2s, 2);
		ServiceInfo serviceInfo = new ServiceInfo();

		System.out.println(parallelize.mapPartitionsWithIndex(
				new Function2<Integer, Iterator<Tuple2<String, Integer>>, Iterator<Tuple2<Integer,Tuple2<String, Integer>>>>() {
			@Override
			public Iterator<Tuple2<Integer, Tuple2<String, Integer>>> call(Integer v1, Iterator<Tuple2<String, Integer>> v2) throws Exception {

				LinkedList<Tuple2<Integer, Tuple2<String, Integer>>> linkedList = new LinkedList<>();
				while (v2.hasNext()) {
					linkedList.add(new Tuple2<>(v1, v2.next()));
				}
				return linkedList.iterator();
			}
		}, false).collect());


		ServiceInfo aggregate = parallelize.aggregate(serviceInfo, new Function2<ServiceInfo, Tuple2<String, Integer>, ServiceInfo>() {
			@Override
			public ServiceInfo call(ServiceInfo v1, Tuple2<String, Integer> v2) throws Exception {

				ServiceInfo serviceInfo1 = new ServiceInfo(v2._2.toString(), v2._1, v1.getServiceType());
				System.out.println(serviceInfo1);
				return serviceInfo1;
			}
		}, new Function2<ServiceInfo, ServiceInfo, ServiceInfo>() {
			@Override
			public ServiceInfo call(ServiceInfo v1, ServiceInfo v2) throws Exception {
				ServiceInfo serviceInfo1 = new ServiceInfo(v1.getServiceCode() + v2.getServiceCode()
						, v1.getServiceName() + v2.getServiceName(),
						v1.getServiceType() + v2.getServiceType());
				System.out.println(serviceInfo1);
				return serviceInfo1;
			}
		});
		System.out.println(aggregate);



		/*Tuple2<String, Integer> zero = new Tuple2<>("D", -1);
		Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> seqOp = new Function2<Tuple2<String, Integer>, Tuple2<String,
				Integer>, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) throws Exception {
				return new Tuple2<>(v1._1+v2._1,v1._2+v2._2);
			}
		};

		Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> combOp = new Function2<Tuple2<String, Integer>, Tuple2<String,
				Integer>, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) throws Exception {
				return new Tuple2<>(v1._1+v2._1,v1._2+v2._2);
			}
		};*/
	}
}
