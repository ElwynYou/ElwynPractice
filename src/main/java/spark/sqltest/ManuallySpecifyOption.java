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
public class ManuallySpecifyOption {
    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf().setMaster("local").setAppName("man");
        SparkContext javaSparkContext = new SparkContext(sparkConf);
        SparkSession sparkSession = new SparkSession(javaSparkContext);

        Dataset<Row> load = sparkSession.read().load("C:\\Users\\Administrator\\Desktop\\student.txt");
        load.printSchema();
        load.show();
        load.write().format("json").save("C:\\Users\\Administrator\\Desktop\\student2.txt");

    }
}
