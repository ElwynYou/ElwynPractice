package spark.udf;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

/**
 * @Name spark.udf.UDF
 * @Description //定义UDAF函数,按年聚合后比较,需要实现UserDefinedAggregateFunction中定义的方法
 * @Author Elwyn
 * @Version 2017/11/10
 * @Copyright 上海云辰信息科技有限公司
 **/
//参考https://yq.aliyun.com/articles/43588
//UDAF与DataFrame列有关的输入样式,StructField的名字并没有特别要求，完全可以认为是两个内部结构的列名占位符。
public class YearOnYearCompare extends UserDefinedAggregateFunction {

	private static final long serialVersionUID = 5840705124061048326L;
	private DateRange previous;
	private DateRange current;

	public YearOnYearCompare() {
	}

	public YearOnYearCompare(DateRange current) {
		this.current = current;
		this.previous = new DateRange(subtractOneYear(current.startDate), subtractOneYear(current.endDate));
	}

	//至于UDAF具体要操作DataFrame的哪个列，取决于调用者，但前提是数据类型必须符合事先的设置，如这里的DoubleType与DateType类型
	@Override
	public StructType inputSchema() {
		List<StructField> structFields = new ArrayList<>();
		structFields.add(DataTypes.createStructField("metric", DataTypes.DoubleType, true));
		structFields.add(DataTypes.createStructField("timeCategory", DataTypes.StringType, true));
		return DataTypes.createStructType(structFields);
	}

	//定义存储聚合运算时产生的中间数据结果的Schema
	@Override
	public StructType bufferSchema() {
		List<StructField> fieldList = new ArrayList<>();
		fieldList.add(DataTypes.createStructField("sumOfCurrent", DataTypes.DoubleType, true));
		fieldList.add(DataTypes.createStructField("sumOfPrevious", DataTypes.DoubleType, true));
		return DataTypes.createStructType(fieldList);
	}

	//标明了UDAF函数的返回值类型
	@Override
	public DataType dataType() {
		return DataTypes.DoubleType;
	}

	//用以标记针对给定的一组输入,UDAF是否总是生成相同的结果
	@Override
	public boolean deterministic() {
		return true;
	}

	//对聚合运算中间结果的初始化
	@Override
	public void initialize(MutableAggregationBuffer mutableAggregationBuffer) {
		mutableAggregationBuffer.update(0, 0.0);
		mutableAggregationBuffer.update(1, 0.0);
	}

	//第二个参数input: Row对应的并非DataFrame的行,而是被inputSchema投影了的行。以本例而言，每一个input就应该只有两个Field的值
	@Override
	public void update(MutableAggregationBuffer mutableAggregationBuffer, Row input) {
		String dateString = input.getString(1);
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date date = null;
		try {
			date = new Date(simpleDateFormat.parse(dateString).getTime());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		if (current.in(date)) {
			mutableAggregationBuffer.update(0, mutableAggregationBuffer.getDouble(0) + input.getDouble(0));
		}
		if (previous.in(date)) {
			mutableAggregationBuffer.update(1, mutableAggregationBuffer.getDouble(1) + input.getDouble(0));
		}
	}

	//负责合并两个聚合运算的buffer，再将其存储到MutableAggregationBuffer中
	@Override
	public void merge(MutableAggregationBuffer mutableAggregationBuffer, Row row) {
		mutableAggregationBuffer.update(0, mutableAggregationBuffer.getDouble(0) + row.getDouble(0));
		mutableAggregationBuffer.update(1, mutableAggregationBuffer.getDouble(1) + row.getDouble(1));
	}

	//完成对聚合Buffer值的运算,得到最后的结果
	@Override
	public Object evaluate(Row row) {
		if (row.getDouble(1) == 0.0) {
			return 0.0;
		} else {
			return (row.getDouble(0) - row.getDouble(1)) / row.getDouble(1) * 100;
		}
	}

	private Timestamp subtractOneYear(Timestamp date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(date.getTime());
		calendar.add(Calendar.YEAR, -1);
		return new Timestamp(calendar.getTimeInMillis());
	}


	//定义一个日期范围类
	static class DateRange implements Serializable {

		private static final long serialVersionUID = -2279749014886223265L;
		private Timestamp startDate;
		private Timestamp endDate;

		public DateRange(Timestamp startDate, Timestamp endDate) {
			this.startDate = startDate;
			this.endDate = endDate;
		}

		public Boolean in(Date targetDate) {
			return targetDate.before(endDate) && targetDate.after(startDate);
		}

		@Override
		public String toString() {
			return "DateRange{" +
					"startDate=" + startDate.toLocaleString() +
					", endDate=" + endDate.toLocaleString() +
					'}';
		}
	}


	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("UADF");
//"id", "name", "sales", "discount", "state", "saleDate"

		List<StructField> fields = new ArrayList<>();


		fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("sales", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("discount", DataTypes.DoubleType, true));
		fields.add(DataTypes.createStructField("state", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("saleDate", DataTypes.StringType, true));

		List<String> strings = Arrays.asList(
				"1, Widget Co, 1000.00, 0.00, AZ, 2014-01-02",
				"2, Acme Widgets, 2000.00, 500.00, CA, 2014-02-01",
				"3, Widgetry, 1000.00, 200.00, CA, 2015-01-11",
				"4, Widgets R Us, 5000.00, 0.0, CA, 2015-02-19",
				"5, Ye Olde Widgete, 4200.00, 0.0, MA, 2015-02-18"
		);
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		JavaRDD<String> parallelize = javaSparkContext.parallelize(strings);
		JavaRDD<Row> map = parallelize.map(v1 -> {
			String[] split = v1.split(",");
			return RowFactory.create(
					Integer.valueOf(split[0]),
					split[1],
					Double.valueOf(split[2]),
					Double.valueOf(split[3]),
					split[4],
					split[5]
			);
		});
		SparkSession sparkSession = SparkSession.builder().getOrCreate();
		StructType structType = DataTypes.createStructType(fields);

		Dataset<Row> dataFrame = sparkSession.createDataFrame(map, structType);
		dataFrame.createOrReplaceTempView("sales");
		dataFrame.printSchema();
		DateRange current = new DateRange(Timestamp.valueOf("2015-01-01 00:00:00"), Timestamp.valueOf("2015-12-31 00:00:00"));
		YearOnYearCompare yearOnYear = new YearOnYearCompare(current);
		sparkSession.udf().register("yearOnYear", yearOnYear);
		Dataset<Row> sql = sparkSession.sql("select yearOnYear(sales, saleDate) as yearOnYear from sales");
		//Dataset<Row> sql = sparkSession.sql("select * from sales");
		sql.show();
	}
}
