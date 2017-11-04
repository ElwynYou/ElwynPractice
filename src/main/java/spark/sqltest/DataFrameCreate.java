package spark.sqltest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * @Package spark.sql
 * @Description:
 * @Author elwyn
 * @Date 2017/11/2 22:46
 * @Email elonyong@163.com
 */
public class DataFrameCreate {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("dataframe");
        JavaSparkContext javaSparkContext=new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        Dataset<Row> json = sqlContext.read().json("hdfs://spark.nebuinfo.com/json");
        json.show();

    }
}
