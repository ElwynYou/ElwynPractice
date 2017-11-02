package spark.sql;

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
public class DataFrameCreateOperation {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("dataframe");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        //创建出来的dataframe可以理解为一个表
        Dataset<Row> df = sqlContext.read().json("hdfs://spark.nebuinfo.com/df");
        //打印DataFrame所有的数据
        df.show();
        //打印dataframe的元数据
        df.printSchema();
        //查询某列所有的数据
        df.select("name").show();
        //查询某几列所有的数据,并对列进行计算
        df.select(df.col("name"), df.col("age").plus(1)).show();
        //根据某一列的值进行过滤
        df.filter(df.col("age").gt(18)).show();
        //根据某一列分组,然后聚合
        df.groupBy(df.col("age")).count().show();

    }
}
