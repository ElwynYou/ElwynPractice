package spark;

import spark.streaming.ConnectionPool;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @Name spark.ServiceInfo
 * @Description
 * @Author Elwyn
 * @Version 2017/11/9
 * @Copyright 上海云辰信息科技有限公司
 **/
public class ServiceInfo implements Serializable {
	private String serviceCode = "";
	private String serviceName = "";
	private String serviceType = "";

	public ServiceInfo() {
	}

	public ServiceInfo(String serviceCode, String serviceName, String serviceType) {
		this.serviceCode = serviceCode;
		this.serviceName = serviceName;
		this.serviceType = serviceType;
	}

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

	@Override
	public String toString() {
		return "ServiceInfo{" +
				"serviceCode='" + serviceCode + '\'' +
				", serviceName='" + serviceName + '\'' +
				", serviceType='" + serviceType + '\'' +
				'}';
	}

	public static void main(String[] args) throws SQLException {
		Connection connection = ConnectionPool.getConnection();
		//遍历partition中的数据,使用一个链接,插入数据库
		String imsi = "fdsafdsafdsa";
		Long nums = 3321L;
		System.out.println(imsi + nums);
		String sql = "MERGE INTO imsi_count a " +
				"USING (SELECT " + imsi + " AS imsi," + nums + " AS nums FROM dual) b " +
				"ON ( a.imsi=b.imsi) " +
				"WHEN MATCHED THEN " +
				"    UPDATE SET a.nums = b.nums " +
				"WHEN NOT MATCHED THEN  " +
				"    INSERT (imsi,nums) VALUES(b.imsi,b.nums)";
		System.out.println(sql);
		Statement statement = connection.createStatement();
		try {
			statement.execute(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		ConnectionPool.returnConnection(connection);

	}

}
