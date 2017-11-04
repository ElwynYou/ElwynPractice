package spark.sqltest;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @Package spark.sqltest
 * @Description:
 * @Author elwyn
 * @Date 2017/11/5 0:34
 * @Email elonyong@163.com
 */
public class JDBCDataSource {
    public static void main(String[] args) {

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "123456");
        connectionProperties.put("driver", "com.mysql.jdbc.Driver");

        SparkSession jdbcDataSource = SparkSession.builder().appName("JDBCDataSource").getOrCreate();
        //加载两个表的数据
        Dataset<Row> studentInfos = jdbcDataSource.read()
                .jdbc("jdbc:mysql://hadoop-senior.ibeifeng.com:3306/testdb", "student_infos", connectionProperties);

        Dataset<Row> studentscores = jdbcDataSource.read().format("jdbc")
                .option("url", "jdbc:mysql://hadoop-senior.ibeifeng.com:3306/testdb")
                .option("dbtable", "student_scores")
                .option("user", "root")
                .option("password", "123456")
                .option("driver", "com.mysql.jdbc.Driver")
                .load();
//将两个dataset转换为javapairRdd进行join操作

        JavaPairRDD<String, Tuple2<Integer, Integer>> studentsRDD = studentInfos.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), Integer.valueOf(String.valueOf(row.get(1))));
            }
        }).join(studentscores.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), Integer.valueOf(String.valueOf(row.get(1))));
            }
        }));
        //javaPairRdd转换为JavaRddrow并且过滤分数小80的
        JavaRDD<Row> studentRowRdd = studentsRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple2) throws Exception {
                return RowFactory.create(tuple2._1, tuple2._2._1, tuple2._2._2);
            }
        }).filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                return row.getInt(2)>80;
            }
        });
        //转换为dataset
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        fields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        fields.add(DataTypes.createStructField("score",DataTypes.IntegerType,true));
        StructType structType = DataTypes.createStructType(fields);
        Dataset<Row> studentsDF = jdbcDataSource.createDataFrame(studentRowRdd, structType);
        //将DataFrame中的数据保存到mysql中
        studentsDF.write().mode(SaveMode.Append)
                .jdbc("jdbc:mysql://hadoop-senior.ibeifeng.com:3306/testdb", "good_student_infos", connectionProperties);
    }
}
