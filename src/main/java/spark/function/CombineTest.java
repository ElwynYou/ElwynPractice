package spark.function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
//计算每个人的平均分
public class CombineTest {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		ArrayList<ScoreDetail> scoreDetails = new ArrayList<>();
		scoreDetails.add(new ScoreDetail("xiaoming", "Math", 98));
		scoreDetails.add(new ScoreDetail("xiaoming", "English", 88));
		scoreDetails.add(new ScoreDetail("wangwu", "Math", 75));
		scoreDetails.add(new ScoreDetail("wangwu", "Englist", 78));
		scoreDetails.add(new ScoreDetail("lihua", "Math", 90));
		scoreDetails.add(new ScoreDetail("lihua", "English", 80));
		scoreDetails.add(new ScoreDetail("zhangsan", "Math", 91));
		scoreDetails.add(new ScoreDetail("zhangsan", "English", 80));

		JavaRDD<ScoreDetail> scoreDetailsRDD = sc.parallelize(scoreDetails);

		JavaPairRDD<String, ScoreDetail> pairRDD = scoreDetailsRDD.mapToPair(new PairFunction<ScoreDetail, String, ScoreDetail>() {
			@Override
			public Tuple2<String, ScoreDetail> call(ScoreDetail scoreDetail) throws Exception {

				return new Tuple2<>(scoreDetail.studentName, scoreDetail);
			}
		});
//        new Function<ScoreDetail, Float,Integer>();

		Function<ScoreDetail, Tuple2<Float, Integer>> createCombine = new Function<ScoreDetail, Tuple2<Float, Integer>>() {
			@Override
			public Tuple2<Float, Integer> call(ScoreDetail scoreDetail) throws Exception {
				return new Tuple2<>(scoreDetail.score, 1);
			}
		};

		// Function2传入两个值，返回一个值
		Function2<Tuple2<Float, Integer>, ScoreDetail, Tuple2<Float, Integer>> mergeValue = new Function2<Tuple2<Float, Integer>, ScoreDetail, Tuple2<Float,
				Integer>>() {
			@Override
			public Tuple2<Float, Integer> call(Tuple2<Float, Integer> tp, ScoreDetail scoreDetail) throws Exception {
				return new Tuple2<>(tp._1 + scoreDetail.score, tp._2 + 1);
			}
		};
		Function2<Tuple2<Float, Integer>, Tuple2<Float, Integer>, Tuple2<Float, Integer>> mergeCombiners = new Function2<Tuple2<Float, Integer>, Tuple2<Float,
                Integer>, Tuple2<Float, Integer>>() {
			@Override
			public Tuple2<Float, Integer> call(Tuple2<Float, Integer> tp1, Tuple2<Float, Integer> tp2) throws Exception {
				return new Tuple2<>(tp1._1 + tp2._1, tp1._2 + tp2._2);
			}
		};

		//createCombiner: (x: ScoreDetail) => (x.score, 1) 转换成分数和次数的tuple2
		//这是第一次遇到zhangsan，创建一个函数，把map中的value转成另外一个类型 ，这里是把(zhangsan,(ScoreDetail类))转换成(zhangsan,(91,1))
		//mergeValue: (acc: (Float, Int), x: ScoreDetail) => (acc._1 + x.score, acc._2 + 1) 再次碰到张三，
		// 就把这两个合并, 这里是将(zhangsan,(91,1)) 这种类型 和 (zhangsan,(ScoreDetail类))这种类型合并，
		// 合并成了(zhangsan,(171,2))
		//mergeCombiners (acc1: (Float, Int), acc2: (Float, Int)) 这个是将多个分区中的zhangsan的数据进行合并，
		// 我们这里zhansan在同一个分区，这个地方就没有用上

		JavaPairRDD<String, Tuple2<Float, Integer>> combineByRDD = pairRDD.combineByKey(createCombine, mergeValue, mergeCombiners);

		//打印平均数
		Map<String, Tuple2<Float, Integer>> stringTuple2Map = combineByRDD.collectAsMap();
		for (String et : stringTuple2Map.keySet()) {
			System.out.println(et + " " + stringTuple2Map.get(et)._1 / stringTuple2Map.get(et)._2);
		}
	}

	static class ScoreDetail implements Serializable {
		//case class ScoreDetail(studentName: String, subject: String, score: Float)
		public String studentName;
		public String subject;
		public float score;

		public ScoreDetail(String studentName, String subject, float score) {
			this.studentName = studentName;
			this.subject = subject;
			this.score = score;
		}
	}
}