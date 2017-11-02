package spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * @Package spark.sql
 * @Description:使用反射的方式把RDD转化为DataFrame
 * @Author elwyn
 * @Date 2017/11/2 23:13
 * @Email elonyong@163.com
 */
public class RDD2DataFrameReflection {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("rdd2dataframe");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        JavaRDD<String> lines = javaSparkContext.textFile("C:\\Users\\Administrator\\Desktop\\student.txt");
        JavaRDD<Student> map = lines.map((Function<String, Student>) s -> {
            String[] split = s.split(",");
            Student student = new Student();
            student.setId(Integer.parseInt(split[0].trim()));
            student.setName(split[1]);
            student.setAge(Integer.parseInt(split[2].trim()));
            return student;
        });
        //使用反射的方式,将Rdd转换为DataFrame
        Dataset<Row> studentDf = sqlContext.createDataFrame(map, Student.class);
        //注册为一个临时表.
        studentDf.registerTempTable("students");
        Dataset<Row> sql = sqlContext.sql("select * from students where age<=18");
        //将查询出来的dataframe再次转换为RDD
        JavaRDD<Row> rowJavaRDD = sql.javaRDD();
        JavaRDD<Student> map1 = rowJavaRDD.map((Function<Row, Student>) row -> {
            //row中顺序可能会跟文件的顺序不一样
            Student student = new Student();
            student.setId(row.getInt(1));
            student.setName(row.getString(2));
            student.setAge(row.getInt(0));
            return student;
        });

        List<Student> collect = map1.collect();
        for (Student student : collect) {
            System.out.println(student);
        }
    }

}
