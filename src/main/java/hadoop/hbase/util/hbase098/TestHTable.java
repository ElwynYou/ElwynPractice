package hadoop.hbase.util.hbase098;

import hadoop.hbase.util.hbase098.HbaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * @Package hadoop.hbase
 * @Description: //todo(用一句话描述该文件做什么)
 * @Author elwyn
 * @Date 2017/8/4 0:20
 * @Email elonyong@163.com
 */
public class TestHTable {
	static byte[] family = Bytes.toBytes("f");

	public static void main(String[] args) throws IOException {
		Configuration configuration = HbaseUtil.getHBaseConfiguration();
		HTable hTable = new HTable(configuration, "users");
		try {
			testscan(hTable);
		} finally {
			hTable.close();
		}
	}

	static void testpool(Configuration configuration) throws IOException {
		ExecutorService executorService= Executors.newFixedThreadPool(2);
		HConnection pool=HConnectionManager.createConnection(configuration,executorService);
		HTableInterface hTableInterface=pool.getTable("users");

	}

	static void testscan(HTableInterface hTable) throws IOException {
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes("row1"));
		scan.setStopRow(Bytes.toBytes("row3"));
		FilterList list =new FilterList(FilterList.Operator.MUST_PASS_ALL);
		byte[] [] prefixes=new byte[2][];
		prefixes[0]=Bytes.toBytes("id");
		prefixes[1]=Bytes.toBytes("name");
		MultipleColumnPrefixFilter multipleColumnPrefixFilter=new MultipleColumnPrefixFilter(prefixes);
		list.addFilter(multipleColumnPrefixFilter);
		scan.setFilter(multipleColumnPrefixFilter);

		ResultScanner scanner = hTable.getScanner(scan);
		for (Result result : scanner) {
			printResult(result);
		}
	}

	static void printResult(Result result) {
		System.out.println("********************************"+Bytes.toString(result.getRow()));
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
		for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMapEntry : map.entrySet()) {
			String family = Bytes.toString(navigableMapEntry.getKey());
			for (Map.Entry<byte[], NavigableMap<Long, byte[]>> mapEntry : navigableMapEntry.getValue().entrySet()) {
				String colum = Bytes.toString(mapEntry.getKey());
				String value="";
				if("age".equals(colum)){
					value = ""+Bytes.toInt(mapEntry.getValue().firstEntry().getValue());

				}else {

					 value = Bytes.toString(mapEntry.getValue().firstEntry().getValue());
				}
				System.out.println(family + ":" + colum + ":" + value);
			}
		}
	}

	private static void testput(HTableInterface hTable) throws IOException {
		//单个put
		Put put = new Put(Bytes.toBytes("row1"));
		put.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("1"));
		put.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
		put.add(Bytes.toBytes("f"), Bytes.toBytes("age"), Bytes.toBytes(27));
		put.add(Bytes.toBytes("f"), Bytes.toBytes("phone"), Bytes.toBytes("165156"));
		put.add(Bytes.toBytes("f"), Bytes.toBytes("email"), Bytes.toBytes("zhangsan@qq.com"));
		hTable.put(put);
//
		Put put1 = new Put(Bytes.toBytes("row2"));
		put1.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("2"));
		put1.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("user2"));

		Put put2 = new Put(Bytes.toBytes("row3"));
		put2.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("3"));
		put2.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("user3"));

		Put put3 = new Put(Bytes.toBytes("row4"));
		put3.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("4"));
		put3.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("user4"));

		List<Put> list = new ArrayList<>();
		list.add(put1);
		list.add(put2);
		list.add(put3);
		hTable.put(list);

		//检测
		Put put4 = new Put(Bytes.toBytes("row4"));
		put4.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("5"));
		hTable.checkAndPut(Bytes.toBytes("row4"), Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("5"), put4);
	}

	static void testget(HTableInterface hTable) throws IOException {
		Get get = new Get(Bytes.toBytes("row1"));
		Result result = hTable.get(get);
		byte[] buf = result.getValue(family, Bytes.toBytes("id"));
		System.out.println("id" + Bytes.toString(buf));
		buf = result.getValue(family, Bytes.toBytes("age"));
		System.out.println("age" + Bytes.toString(buf));
		buf = result.getValue(family, Bytes.toBytes("name"));
		System.out.println("name" + Bytes.toString(buf));
		buf = result.getRow();
		System.out.println("row" + Bytes.toString(buf));
	}

	static void testDelete(HTableInterface hTable) throws IOException {
		Delete delete = new Delete(Bytes.toBytes("row4"));
		delete.deleteColumn(family, Bytes.toBytes("id"));
		delete.deleteColumn(family, Bytes.toBytes("name"));
		//delete.deleteFamily(family);
		hTable.delete(delete);
	}
}
