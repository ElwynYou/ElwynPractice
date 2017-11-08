package spark.streaming;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

public class ConnectionPool {
	private static LinkedList<Connection> connectionQueue;

	static {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public synchronized static Connection getConnection() {
		try {
			if (connectionQueue == null) {
				connectionQueue = new LinkedList<Connection>();
				for (int i = 0; i < 5; i++) {
					Connection conn = DriverManager.getConnection(
							"jdbc:oracle:thin:@192.168.30.205:1521/rsdb",//192.168.60.96  rain_soft2013
							"gz_inner",
							"gz_inner");
					connectionQueue.push(conn);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return connectionQueue.poll();

	}

	public static void returnConnection(Connection conn) {
		connectionQueue.push(conn);
	}

	public static void main(String[] args) {
		System.out.println(getConnection());

	}

}