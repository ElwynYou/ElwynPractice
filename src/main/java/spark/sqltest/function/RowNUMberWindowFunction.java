package spark.sqltest.function;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @Package spark.sqltest.function
 * @Description:
 * @Author elwyn
 * @Date 2017/11/5 22:33
 * @Email elonyong@163.com
 */
public class RowNUMberWindowFunction {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "beifeng");
        SparkSession rowNUMberWindowFunction = SparkSession.builder().appName("RowNUMberWindowFunction").enableHiveSupport()
                .getOrCreate();
        rowNUMberWindowFunction.sql("drop table if exists sales");
        rowNUMberWindowFunction.sql("create table if not exists sales (" +
                "product STRING," +
                "category STRING," +
                "revenue BIGINT)");
        rowNUMberWindowFunction.sql("load data " +
                "inpath '/usr/local/spark-study/sales.txt' " +
                "into table sales");
        //开始编写统计逻辑,使用row_number()开窗函数
        //row_number开窗函数的作用
        //给每个分组的数据,按照其排序顺序打上一个分组内的行号
        Dataset<Row> top3 = rowNUMberWindowFunction.sql("select product,category,revenue " +
                "from ( select product,category,revenue," +
                "row_number() over(partition by category order by revenue desc) rank" + " from sales )tmp_sales where rank <=3"
        );
        //将每组排名前三的数据保存到一个表中
        rowNUMberWindowFunction.sql("drop table if exists top3_sales");
        top3.write().saveAsTable("top3_sales");
    }
}
