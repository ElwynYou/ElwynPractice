package spark.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
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
import org.apache.hadoop.hbase.util.Triple;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import scala.Function1;
import scala.Tuple2;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * @Name spark.analysis.AdjointAnalysis
 * @Description
 * @Author Elwyn
 * @Version 2017/11/10
 * @Copyright 上海云辰信息科技有限公司
 **/
public class SparkHbase  {
	public static void main(String[] args) throws IOException {
		SparkConf sc = new SparkConf().setAppName("SparkHbase").setMaster("local").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sc);
		//不能用spark的javaSparkContext.hadoopConfiguration()获取conf
		Configuration conf =HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "bigdata01.nebuinfo.com");

		//conf.addResource(new Path("file:///etc/hbase/conf/hbase-site.xml"));
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("info"));
		conf.set(TableInputFormat.INPUT_TABLE, "student");//设置查询的表
		conf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()));//设置扫描的列
		conf.set(TableOutputFormat.OUTPUT_TABLE, "student");

		//必须要利用conf创建Job对象,并设置输出格式,否则会出异常
		//Exception in thread "main" org.apache.hadoop.mapred.InvalidJobConfException: Output directory not set.
		Job newAPIJobConfiguration = Job.getInstance(conf);
		//newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "student");
		newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);




		JavaPairRDD<ImmutableBytesWritable, Result> hbaseRdd = javaSparkContext.newAPIHadoopRDD(conf, TableInputFormat.class,
				ImmutableBytesWritable.class, Result.class).cache();

		List<Tuple2<ImmutableBytesWritable, Result>> collect = hbaseRdd.collect();
		for (Tuple2<ImmutableBytesWritable, Result> tuple : collect) {
			showCell(tuple._2);
		}

		List<Triple<String, Integer, Integer>> triples = Arrays.asList(new Triple<String, Integer, Integer>("10", 12321, 321)
				, new Triple<String, Integer, Integer>("11", 4444, 5555));
		JavaRDD<Triple<String, Integer, Integer>> parallelize = javaSparkContext.parallelize(triples);


		parallelize.mapToPair(new PairFunction<Triple<String, Integer, Integer>, ImmutableBytesWritable, Put>() {
			@Override
			public Tuple2<ImmutableBytesWritable, Put> call(Triple<String, Integer, Integer> stringIntegerIntegerTriple) throws Exception {
				Put put = new Put(Bytes.toBytes("3"));
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("1fdsa"));
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("33"));
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("gender"), Bytes.toBytes("44"));
				return new Tuple2<>(new ImmutableBytesWritable(), put);
			}
		}).saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());


	}

	/**
	 * 格式化输出
	 *
	 * @param result
	 */
	private static void showCell(Result result) {
		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			System.out.println("RowName: " + new String(CellUtil.cloneRow(cell)) + " ");
			System.out.println("Timetamp: " + cell.getTimestamp() + " ");
			System.out.println("column Family: " + new String(CellUtil.cloneFamily(cell)) + " ");
			System.out.println("row Name: " + new String(CellUtil.cloneQualifier(cell)) + " ");
			System.out.println("value: " + new String(CellUtil.cloneValue(cell)) + " ");
		}
	}


}
