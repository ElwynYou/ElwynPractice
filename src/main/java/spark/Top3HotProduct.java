package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * @Package spark
 * @Description:
 * @Author elwyn
 * @Date 2017/11/9 20:52
 * @Email elonyong@163.com
 */
public class Top3HotProduct {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Top3HotProduct");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(1));
        //利用checkpoint提高容错和可恢复
        //javaStreamingContext.checkpoint("hdfs文件系统");

        //输入日志的格式 leo iphone mobile_phone
        //首先获取输入数据流
        JavaReceiverInputDStream<String> stringJavaReceiverInputDStream = javaStreamingContext.socketTextStream("hadoop-senior.ibeifeng.com", 9999);
        //然后做映射,映射为(category_product,1)
        JavaPairDStream<String, Integer> pairDStream = stringJavaReceiverInputDStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] split = s.split(" ");
                return new Tuple2<>(split[2] + "_" + split[1], 1);
            }
        });
        //然后执行window操作
        //每隔10秒,对最近60秒的数据,执行reduceByKey的操作
        //计算出来这60秒内,每个种类的每个商品的点击次数
        pairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.seconds(60), Durations.seconds(10))

                //然后针对60秒内的每个种类的每个商品的点击次数
                //foreachRDD
                .foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
                    @Override
                    public void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {
                        boolean empty = stringIntegerJavaPairRDD.isEmpty();
                        if (empty) {
                            System.out.println("空的");
                            return;
                        }
                        //将该rdd,转化为javardd<row>的格式
                        JavaRDD<Row> map = stringIntegerJavaPairRDD.map(new Function<Tuple2<String, Integer>, Row>() {
                            @Override
                            public Row call(Tuple2<String, Integer> v1) throws Exception {
                                String category = v1._1.split("_")[0];
                                String product = v1._1.split("_")[1];
                                Integer count = v1._2;
                                return RowFactory.create(category, product, count);
                            }
                        });
                        //然后执行dataFrame转换
                        ArrayList<StructField> structFields = new ArrayList<>();
                        structFields.add(DataTypes.createStructField("category", DataTypes.StringType, true));
                        structFields.add(DataTypes.createStructField("product", DataTypes.StringType, true));
                        structFields.add(DataTypes.createStructField("click_count", DataTypes.IntegerType, true));
                        StructType structType = DataTypes.createStructType(structFields);
                        //SQLContext sqlContext = new SQLContext();老api应该用hivecontext
                        SparkSession sparkSession = new SparkSession(stringIntegerJavaPairRDD.context());
                        Dataset<Row> dataFrame = sparkSession.createDataFrame(map, structType);
                        //将60秒内的每个种类的每个商品的点击次数注册为临时表
                        dataFrame.registerTempTable("product_click_log");
                        sparkSession.sql("select category,product,click_count " +
                                "from (" +
                                "select category,product,click_count," +
                                "row_number() over (partition by category order by click_count desc) rank " +
                                "from product_click_log" +
                                ") tmp " +
                                "where rank <=3").show();
                    }
                });
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
        javaStreamingContext.close();
    }
}
