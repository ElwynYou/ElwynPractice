package spark.sqltest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @Package spark.sql
 * @Description:以编程方式动态指定元数据,将RDD转换为DataFrame
 * @Author elwyn
 * @Date 2017/11/2 23:41
 * @Email elonyong@163.com
 */
public class RDD2DataFrameProgrammatically {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("rddp");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        JavaRDD<String> lines = javaSparkContext.textFile("C:\\Users\\Administrator\\Desktop\\student.txt");
        JavaRDD<Row> rows = lines.map((Function<String, Row>) s -> {
            String[] split = s.split(",");
            return RowFactory.create(Integer.valueOf(split[0]),split[1],Integer.valueOf(split[2]));
        });
        //动态构造元数据
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(fields);
        //使用动态构造的元数据,将RDD转换为DataFrame
        Dataset<Row> dataFrame = sqlContext.createDataFrame(rows, structType);
        dataFrame.registerTempTable("students");
        Dataset<Row> sql = sqlContext.sql("select * from students where age <=18");
        List<Row> collect = sql.javaRDD().collect();
        for (Row row : collect) {
            System.out.println(row);
        }
    }
}
