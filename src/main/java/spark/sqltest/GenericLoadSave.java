package spark.sqltest;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @Package spark.sql
 * @Description:通用的load和save操作
 * @Author elwyn
 * @Date 2017/11/4 0:18
 * @Email elonyong@163.com
 */
public class GenericLoadSave {
    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf().setMaster("local");
        SparkContext javaSparkContext = new SparkContext(sparkConf);
        SparkSession sparkSession = new SparkSession(javaSparkContext);

        Dataset<Row> load = sparkSession.read().load();
        load.printSchema();
        load.show();
        load.select("sfsda");
        load.write().save();

    }
}
