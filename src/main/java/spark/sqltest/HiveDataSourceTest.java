package spark.sqltest;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import java.util.List;

/**
 * @Package spark.sql
 * @Description:
 * @Author elwyn
 * @Date 2017/11/4 22:42
 * @Email elonyong@163.com
 */
public class HiveDataSourceTest {
	public static void main(String[] args) {
		/*SparkSession hiveDataSource =
				SparkSession.builder()
						.appName("HiveDataSource")
						.enableHiveSupport()
						.getOrCreate();*/
		SparkConf sparkConf = new SparkConf().setAppName("HiveDataSourceTest");
		SparkContext sparkContext = new SparkContext(sparkConf);
		HiveContext hiveDataSource=new HiveContext(sparkContext);
		//如果存在先删除
		hiveDataSource.sql("drop table if exists  student_infos");
		//创建表
		hiveDataSource.sql("create table if not exists  student_infos (name STRING,age INT)");
		//写入学生基本信息数据
		hiveDataSource.sql("LOAD DATA " +
				"INPATH '/sparktest/hive/student_infos.txt' " +
				" INTO TABLE student_infos ");

		//同样方法写入分数表
		hiveDataSource.sql("drop table if exists  student_scores");
		hiveDataSource.sql("create table if not exists  student_scores (name STRING,score INT)");
		hiveDataSource.sql("LOAD DATA " +
				"INPATH '/sparktest/hive/student_scores.txt' " +
				"INTO TABLE student_scores");
//查询大于80分的学生信息
		Dataset<Row> sql = hiveDataSource.sql("select si.name,si.age,ss.score " +
				"from student_infos si " +
				"join student_scores ss on si.name=ss.name " +
				"where ss.score >= 80");
		//然后将查询的数据保存到good_student_infos 表中
		hiveDataSource.sql("drop table if exists  good_student_infos");
		sql.write().saveAsTable("good_student_infos");
		//然后针对good_student_infos表直接创建dataframe
		List<Row> good_student_infos = hiveDataSource.table("good_student_infos").javaRDD().collect();
		for (Row good_student_info : good_student_infos) {
			System.out.println(good_student_info);
		}
	}
}
