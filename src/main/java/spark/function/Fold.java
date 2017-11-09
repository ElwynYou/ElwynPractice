package spark.function;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @Name spark.function.Fold
 * @Description 如果我们把集合看成是一张纸条，每一小段代表一个元素，那么reduceLeft就将这张纸条从左向右”折叠”，
 * 最前面的两个元素会首先“重叠”在一起，这时会使用传给reduceLeft的参数函数进行计算，返回的结果就像是已经折叠在一起的两段纸条，
 * 它们已经是一个叠加的状态了，所以[它]，
 * 也就是上次重叠的结果会继续做为一个单一的值和下一个元素继续“叠加”，直到折叠到集合的最后一个元素
 * Fold类似于reduceLeft, 不过开始折叠的第一个元素不是集合中的第一个元素，
 * 而是传入的一个元素，有点类似于先将这个参数放入的集合中的首位，然后在reduceLeft
 * <p>
 * fold()算子,先用初始值和rdd中的每一个分区进行function2函数计算并聚合,从而使每一个分区得到一个新的值
 * 然后再把每个分区的值再调用function2函数计算,得到新的值
 * 最后用新的值再和初始值调用function2函数计算,得到最终结果
 * @Author Elwyn
 * @Version 2017/11/9
 * @Copyright 上海云辰信息科技有限公司
 **/
public class Fold {

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

		Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> fold = new Function2<Tuple2<String, Integer>, Tuple2<String,
				Integer>, Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
				System.out.println("t1: " + t1);
				System.out.println("t2: " + t2);
				Tuple2<String, Integer> stringIntegerTuple2 = new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2);
				System.out.println(stringIntegerTuple2);
				return stringIntegerTuple2;
			}
		};

		Tuple2<String, Integer> d = parallelize.fold(new Tuple2<>("D", -1), fold);
		System.out.println(d);

		List<Integer> data = Arrays.asList(5, 1, 1, 3, 6, 2, 2);
		JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(data, 5);
		System.out.println(javaRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
			@Override
			public Iterator<String> call(Integer v1, Iterator<Integer> v2) throws Exception {
				LinkedList<String> strings = new LinkedList<>();
				while (v2.hasNext()) {
					strings.add("分区号" + v1 + "数字" + String.valueOf(v2.next()));
				}
				return strings.iterator();
			}
		}, false).collect());


		Integer foldRDD = javaRDD.fold(1, new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" + foldRDD);


		List<String> data1 = Arrays.asList("5", "1", "1", "3", "6", "2", "2");
		JavaRDD<String> javaRDD1 = javaSparkContext.parallelize(data1, 5);
		JavaRDD<String> partitionRDD1 = javaRDD1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
			@Override
			public Iterator<String> call(Integer v1, Iterator<String> v2) throws Exception {
				LinkedList<String> linkedList = new LinkedList<String>();
				while (v2.hasNext()) {
					linkedList.add(v1 + "=" + v2.next());
				}
				return linkedList.iterator();
			}
		}, false);

		System.out.println(partitionRDD1.collect());

		String foldRDD1 = javaRDD1.fold("0", new Function2<String, String, String>() {
			@Override
			public String call(String v1, String v2) throws Exception {
				return v1 + " - " + v2;
			}
		});
		System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + foldRDD1);

		List<Tuple2<Tuple2<String, String>, Tuple2<String, String>>> tuple2s1 =
				Arrays.asList(
						new Tuple2<>(new Tuple2<>("1", "1"), new Tuple2<>("1", "1")),
						new Tuple2<>(new Tuple2<>("2", "2"), new Tuple2<>("2", "2")),
						new Tuple2<>(new Tuple2<>("2", "2"), new Tuple2<>("2", "2"))
				);
		JavaRDD<Tuple2<Tuple2<String, String>, Tuple2<String, String>>> parallelize1 = javaSparkContext.parallelize(tuple2s1);
		JavaRDD<String> map = parallelize1.map(new Function<Tuple2<Tuple2<String, String>, Tuple2<String, String>>, String>() {
			@Override
			public String call(Tuple2<Tuple2<String, String>, Tuple2<String, String>> v1) throws Exception {
				return null;
			}
		});
		JavaRDD<String> stringJavaRDD = parallelize1.flatMap(new FlatMapFunction<Tuple2<Tuple2<String, String>, Tuple2<String, String>>, String>() {
			@Override
			public Iterator<String> call(Tuple2<Tuple2<String, String>, Tuple2<String, String>> tuple2Tuple2Tuple2) throws Exception {
				return null;
			}
		});


	/*	JavaPairRDD<String, Tuple2<String, String>> join = stringStringJavaPairRDD.join(stringStringJavaPairRDD);

		System.out.println(map.collect());

		System.out.println("rdd collect" + rdd.collect());
		System.out.println("rdd count" + rdd.count());
		System.out.println("rdd countByValue" + rdd.countByValue());
		System.out.println("rdd take" + rdd.take(2));
		System.out.println("rdd top" + rdd.top(2));
		System.out.println("rdd takeOrdered" + rdd.takeOrdered(2));
		System.out.println("rdd reduce" + rdd.reduce((x, y) -> x + y));
		System.out.println("rdd fold" + rdd.fold(0, (x, y) -> x + y));*/
	}
}
