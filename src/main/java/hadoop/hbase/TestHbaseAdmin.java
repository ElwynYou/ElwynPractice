package hadoop.hbase;

import hadoop.hbase.util.HbaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * @Package hadoop.hbase
 * @Description: //todo(用一句话描述该文件做什么)
 * @Author elwyn
 * @Date 2017/8/3 22:58
 * @Email elonyong@163.com
 */
public class TestHbaseAdmin {
    public static void main(String[] args) throws IOException {
        Configuration hBaseConfiguration = HbaseUtil.getHBaseConfiguration();
        HBaseAdmin hBaseAdmin = new HBaseAdmin(hBaseConfiguration);
        try {
            testGetTableDescribe(hBaseAdmin);

        } finally {
            hBaseAdmin.close();

        }
    }

    static void testCreateTable(HBaseAdmin hBaseAdmin) throws IOException {
        TableName tableName = TableName.valueOf("users");
        HTableDescriptor hTableDescripto = new HTableDescriptor(tableName);
        hTableDescripto.addFamily(new HColumnDescriptor("f"));
        hTableDescripto.setMaxFileSize(10000L);
        hBaseAdmin.createTable(hTableDescripto);
        System.out.println("创建表成功");

    }

    static void testGetTableDescribe(HBaseAdmin hBaseAdmin) throws IOException {
        TableName tableName = TableName.valueOf("users");
        HTableDescriptor tableDescriptor;
        if (hBaseAdmin.tableExists(tableName)) {
            tableDescriptor = hBaseAdmin.getTableDescriptor(tableName);
            System.out.println(tableDescriptor);

        } else {
            System.out.println("表不存在");
        }
    }

    static void testDelTableDescribe(HBaseAdmin hBaseAdmin) throws IOException {
        TableName tableName = TableName.valueOf("users");
        if (hBaseAdmin.tableExists(tableName)) { //判断表是否存在
            if (hBaseAdmin.isTableEnabled(tableName)) {//判断表的状态是enable还是disable
                hBaseAdmin.disableTable(tableName);
            }
            hBaseAdmin.deleteTable(tableName);
            System.out.println("删除成功");
        } else {
            System.out.println("表不存在");
        }

    }
}
