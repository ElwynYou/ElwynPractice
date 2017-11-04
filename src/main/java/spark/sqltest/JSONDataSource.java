package spark.sqltest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @Package spark.sql
 * @Description:
 * @Author elwyn
 * @Date 2017/11/4 0:55
 * @Email elonyong@163.com
 */
public class JSONDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JSONDataSource").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sparkContext);
        //针对json文件,创建DataFrame
        Dataset<Row> studentScoresDF = sqlContext.read().option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").json("C:\\Users\\Administrator\\Desktop\\student.studentScoresDF");
        //针对学生信息创建临时表,查询分数大于80分的学生姓名
        studentScoresDF.registerTempTable("student_scores");
        Dataset<Row> goodStudentNames = sqlContext.sql("select name,score from student_scores where score>=80");
        List<String> collect = goodStudentNames.javaRDD().map((Function<Row, String>) row -> row.getString(0)).collect();
        //针对JavaRDD,创建DataFrame
        ArrayList<String> studentinfoJsons = new ArrayList<>();
        studentinfoJsons.add("{\"name\":\"leo\",\"age\":18}");
        studentinfoJsons.add("{\"name\":\"Marry\",\"age\":17}");
        studentinfoJsons.add("{\"name\":\"Jack\",\"age\":19}");
        JavaRDD<String> parallelize = sparkContext.parallelize(studentinfoJsons);
        Dataset<Row> studentInfo = sqlContext.read().option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").json(parallelize);
       //针对学生基本信息DataFrame,注册临时表,然后查询分数大于80分的学生基本信息
        studentInfo.registerTempTable("student_info");
        StringBuilder sqlString = new StringBuilder("select name,age from student_info where name in (");
        for (int i = 0; i < collect.size(); i++) {
            sqlString.append("'").append(collect.get(i)).append("'");
            if (i < collect.size() - 1) {
                sqlString.append(",");
            }
        }
        sqlString.append(")");
        Dataset<Row> goodinfo = sqlContext.sql(sqlString.toString());
        //将两分数据的DataFrame,转换为JavaPairRdd,执行join transformation
        JavaPairRDD<String, Tuple2<Integer, Integer>> students = goodStudentNames.javaRDD().mapToPair((PairFunction<Row, String, Integer>) row -> new Tuple2<>(row.getString(0),Integer.valueOf(String.valueOf(row.getLong(1))))).join(goodinfo.javaRDD().mapToPair((PairFunction<Row, String, Integer>) row -> new Tuple2<>(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1))))));
        JavaRDD<Row> map = students.map((Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>) tuple2 -> RowFactory.create(tuple2._1, tuple2._2._1, tuple2._2._2));
        //创建一份元数据，将JavaRDD<Row> 转换
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(fields);
        Dataset<Row> dataFrame = sqlContext.createDataFrame(map, structType);
        dataFrame.write().option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").format("studentScoresDF").save("C:\\Users\\Administrator\\Desktop\\good.studentScoresDF");
    }
}
