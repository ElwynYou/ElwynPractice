package hadoop.hbase.util;

import java.io.IOException;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public enum HBaseUtil2 {

	instance;

	private static Configuration configuration;
	private static Connection connection;
	private static Admin admin;
	private static ExecutorService pool = Executors.newScheduledThreadPool(20);

	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		//配置zookeeper地址,多个用逗号分隔
		configuration.set("hbase.zookeeper.quorum", "hadoop-master.nebuinfo.com");
		/*configuration.set("hbase.master", "hdfs://10.68.128.215:60000");
		configuration.set("hbase.root.dir", "hdfs://10.68.128.215:9000/hbase");*/
		try {
			connection = ConnectionFactory.createConnection(configuration, pool);
			admin = connection.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	/**
	 * 关闭连接
	 */
	public void close() {
		try {
			if (null != admin) {
				admin.close();
			}
			if (null != connection) {
				connection.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 创建表
	 *
	 * @param tableName 表名
	 * @param family    列族列表
	 * @throws IOException
	 */
	public void createTable(String tableName, String[] family) throws IOException {
		TableName tName = TableName.valueOf(tableName);
		if (admin.tableExists(tName)) {
			println(tableName + " exists.");
		} else {
			HTableDescriptor hTableDesc = new HTableDescriptor(tName);
			for (String col : family) {
				HColumnDescriptor hColumnDesc = new HColumnDescriptor(col);
				hTableDesc.addFamily(hColumnDesc);
			}
			admin.createTable(hTableDesc);
		}

	}

	/**
	 * 删除表
	 *
	 * @param tableName 表名称
	 * @throws IOException
	 */
	public void deleteTable(String tableName) throws IOException {
		TableName tName = TableName.valueOf(tableName);
		if (admin.tableExists(tName)) {
			if (admin.isTableEnabled(tName)) {
				admin.disableTable(tName);
			}
			admin.deleteTable(tName);
		} else {
			println(tableName + " not exists.");
		}
	}

	/**
	 * 查看已有表
	 *
	 * @throws IOException
	 */
	public void listTables() {
		HTableDescriptor hTableDescriptors[] = null;
		try {
			hTableDescriptors = admin.listTables();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (hTableDescriptors != null) {
			for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
				println(hTableDescriptor.getNameAsString());
			}
		}

	}

	/**
	 * 插入单行
	 *
	 * @param tableName 表名称
	 * @param rowKey    RowKey
	 * @param colFamily 列族
	 * @param col       列
	 * @param value     值
	 * @throws IOException
	 */
	public static void insert(String tableName, String rowKey, String colFamily, String col, String value) throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(value));
		table.put(put);

		/*
		 * 批量插入 List<Put> putList = new ArrayList<Put>(); puts.add(put); table.put(putList);
		 */

		table.close();
	}

	public void delete(String tableName, String rowKey, String colFamily, String col) throws IOException {

		if (!admin.tableExists(TableName.valueOf(tableName))) {
			println(tableName + " not exists.");
		} else {
			Table table = connection.getTable(TableName.valueOf(tableName));
			Delete del = new Delete(Bytes.toBytes(rowKey));
			if (colFamily != null) {
				del.addFamily(Bytes.toBytes(colFamily));
			}
			if (colFamily != null && col != null) {
				del.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
			}
			/*
			 * 批量删除 List<Delete> deleteList = new ArrayList<Delete>(); deleteList.add(delete); table.delete(deleteList);
			 */
			table.delete(del);
			table.close();
		}
	}

	/**
	 * 根据RowKey获取数据
	 *
	 * @param tableName 表名称
	 * @param rowKey    RowKey名称
	 * @param colFamily 列族名称
	 * @param col       列名称
	 * @throws IOException
	 */
	public void getData(String tableName, String rowKey, String colFamily, String col) throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(rowKey));
		if (colFamily != null) {
			get.addFamily(Bytes.toBytes(colFamily));
		}
		if (colFamily != null && col != null) {
			get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
		}
		Result result = table.get(get);
		showCell(result);
		table.close();
	}

	/**
	 * 根据RowKey获取信息
	 *
	 * @param tableName
	 * @param rowKey
	 * @throws IOException
	 */
	public void getData(String tableName, String rowKey) throws IOException {
		getData(tableName, rowKey, null, null);
	}

	/**
	 * 格式化输出
	 *
	 * @param result
	 */
	private static void showCell(Result result) {
		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			println("RowName: " + new String(CellUtil.cloneRow(cell)) + " ");
			println("Timetamp: " + cell.getTimestamp() + " ");
			println("column Family: " + new String(CellUtil.cloneFamily(cell)) + " ");
			println("row Name: " + new String(CellUtil.cloneQualifier(cell)) + " ");
			println("value: " + new String(CellUtil.cloneValue(cell)) + " ");
		}
	}

	/**
	 * 打印
	 *
	 * @param obj 打印对象
	 */
	private static void println(Object obj) {
		System.out.println(obj);
	}
}
