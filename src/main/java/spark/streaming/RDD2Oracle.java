package spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 * @Name spark.streaming.RDD2Oracle
 * @Description
 * @Author Elwyn
 * @Version 2017/11/8
 * @Copyright 上海云辰信息科技有限公司
 **/
public class RDD2Oracle {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setAppName("OnlineForeachRDD2DB");
		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

		JavaReceiverInputDStream<String> lines = javaStreamingContext.socketTextStream("bigdata05.nebuinfo.com", 9999);


		lines.map(new Function<String, ServiceInfo>() {
			@Override
			public ServiceInfo call(String s) throws Exception {
				String[] split = s.split(",");
				ServiceInfo serviceInfo = new ServiceInfo();
				serviceInfo.setServiceCode(split[0]);
				serviceInfo.setServiceName(split[1]);
				serviceInfo.setServiceType(split[2]);
				return serviceInfo;
			}
		}).foreachRDD(new VoidFunction<JavaRDD<ServiceInfo>>() {
			@Override
			public void call(JavaRDD<ServiceInfo> serviceInfoJavaRDD) throws Exception {
				serviceInfoJavaRDD.foreachPartition(new VoidFunction<Iterator<ServiceInfo>>() {
					@Override
					public void call(Iterator<ServiceInfo> serviceInfoIterator) throws Exception {
						PreparedStatement preparedStatement = null;
						while (serviceInfoIterator.hasNext()) {
							ServiceInfo next = serviceInfoIterator.next();
							String sql = "INSERT INTO service_info (service_code,service_name,service_type) VALUES(?,?,?)";
							Connection connection = ConnectionPool.getConnection();
							connection.setAutoCommit(false);
							try {
								preparedStatement = connection.prepareStatement(sql);
								preparedStatement.setString(1, next.getServiceCode());
								preparedStatement.setString(2, next.getServiceName());
								preparedStatement.setString(3, next.getServiceType());
								preparedStatement.addBatch();
							} catch (SQLException e) {
								e.printStackTrace();
							} finally {
								connection.setAutoCommit(true);
								ConnectionPool.returnConnection(connection);
							}
						}
						if (preparedStatement != null) {
							preparedStatement.executeBatch();
						}
					}
				});
			}
		});

		javaStreamingContext.start();
		javaStreamingContext.awaitTermination();
		javaStreamingContext.close();


	}

	private static class ServiceInfo {
		private String serviceCode;
		private String serviceName;
		private String serviceType;

		public String getServiceCode() {
			return serviceCode;
		}

		public void setServiceCode(String serviceCode) {
			this.serviceCode = serviceCode;
		}

		public String getServiceName() {
			return serviceName;
		}

		public void setServiceName(String serviceName) {
			this.serviceName = serviceName;
		}

		public String getServiceType() {
			return serviceType;
		}

		public void setServiceType(String serviceType) {
			this.serviceType = serviceType;
		}
	}
}
