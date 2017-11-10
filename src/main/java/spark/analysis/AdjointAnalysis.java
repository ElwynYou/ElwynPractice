package spark.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import scala.Function1;
import scala.Tuple2;
import scala.runtime.BoxedUnit;

import java.io.IOException;

/**
 * @Name spark.analysis.AdjointAnalysis
 * @Description
 * @Author Elwyn
 * @Version 2017/11/10
 * @Copyright 上海云辰信息科技有限公司
 **/
public class AdjointAnalysis {
	public static void main(String[] args) {
		SparkContext sc = new SparkContext("local", "SparkOnHBase");

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", "master");


		// ======Save RDD to HBase========
		// step 1: JobConf setup
		JobConf jobConf = new JobConf(conf);
		jobConf.setOutputFormat(TableOutputFormat.class);
		jobConf.set(TableOutputFormat.OUTPUT_TABLE, "user");


	/*	// step 2: rdd mapping to table

		// 在 HBase 中表的 schema 一般是这样的
		// *row   cf:col_1    cf:col_2
		// 而在Spark中，我们操作的是RDD元组，比如(1,"lilei",14) , (2,"hanmei",18)
		// 我们需要将 *RDD[(uid:Int, name:String, age:Int)]* 转换成 *RDD[(ImmutableBytesWritable, Put)]*
		// 我们定义了 convert 函数做这个转换工作
		public void  convert(triple: (Int, String, Int)) = {
			val p = new Put(Bytes.toBytes(triple._1))
			p.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("name"),Bytes.toBytes(triple._2))
			p.addColumn(Bytes.toBytes("basic"),Bytes.toBytes("age"),Bytes.toBytes(triple._3))
			(new ImmutableBytesWritable, p)
		}

		// step 3: read RDD data from somewhere and convert
		val rawData = List((1,"lilei",14), (2,"hanmei",18), (3,"someone",38))
		val localData = sc.parallelize(rawData).map(convert)

		//step 4: use `saveAsHadoopDataset` to save RDD to HBase
		localData.saveAsHadoopDataset(jobConf)*/
		// =================================


		// ======Load RDD from HBase========
		// use `newAPIHadoopRDD` to load RDD from HBase
		//直接从 HBase 中读取数据并转成 Spark 能直接操作的 RDD[K,V]

		//设置查询的表名
		conf.set(TableInputFormat.INPUT_TABLE, "user");

		//添加过滤条件，年龄大于 18 岁
		Scan scan = new Scan();
		scan.setFilter(new SingleColumnValueFilter("basic".getBytes(), "age".getBytes(),
				CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(18)));

		conf.set(TableInputFormat.SCAN, convertScanToString(scan));

		RDD<Tuple2<ImmutableBytesWritable, Result>> usersRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,
				ImmutableBytesWritable.class,
				Result.class);

		long count = usersRDD.count();
		System.out.println("Users RDD Count:" + count);
		usersRDD.cache();
/*
		//遍历输出
		usersRDD.foreach(new Function1<Tuple2<ImmutableBytesWritable, Result>, BoxedUnit>() {
		}); {
			case (_,result) =>
				val key = Bytes.toInt(result.getRow)
				val name = Bytes.toString(result.getValue("basic".getBytes, "name".getBytes))
				val age = Bytes.toInt(result.getValue("basic".getBytes, "age".getBytes))
				println("Row key:" + key + " Name:" + name + " Age:" + age)
		}*/
	}

	private static String  convertScanToString( Scan scan){
		try {

			ClientProtos.Scan scan1 = ProtobufUtil.toScan(scan);
			return Base64.encodeBytes(scan1.toByteArray());
		}catch (Exception e){
			e.printStackTrace();
			return "";
		}
	}
}
