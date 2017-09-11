package hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.common.IOUtils;

import java.io.IOException;

/**
 * @Package hadoop.hbase
 * @Description: hbase练习
 * @Author elwyn
 * @Date 2017/7/21 1:54
 * @Email elonyong@163.com
 */
public class HBaseOperation {
    public static HTable getHTableByTableName(String tableName) throws IOException {
        //读配置文件
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop-senior.ibeifeng.com");  //增加了这项
        //得到表的实例
        HTable table = new HTable(configuration, tableName);
        return table;
    }

    public void getData() throws IOException {
        String tableName = "user"; //实际要加命名空间 default:user
        HTable table = getHTableByTableName(tableName);
        //创建get 用rowkey
        Get get = new Get(Bytes.toBytes("10002"));
        //添加列簇条件
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));

        //获取数据
        Result result = table.get(get);
        //key:rowkey+cf+c+version
        //value:value

        for (Cell cell : result.rawCells()) {
            System.out.println(
                    Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
                            + Bytes.toString(CellUtil.cloneQualifier(cell)) + "->"
                            + Bytes.toString(CellUtil.cloneValue(cell))
            );
        }

        table.close();
    }

    /**
     * 建议
     *  tablename & columnfamily  写成常量，HBaseTableContent
     *  Map<String,Object>
     * @throws IOException
     */

    public void putData() throws IOException {
        String tableName = "user"; //实际要加命名空间 default:user
        HTable table = getHTableByTableName(tableName);

        Put put = new Put(Bytes.toBytes("10004"));
        put.add(Bytes.toBytes("info"),
                Bytes.toBytes("name"),
                Bytes.toBytes("hehe"));

        put.add(Bytes.toBytes("info"),
                Bytes.toBytes("age"),
                Bytes.toBytes(25));

        put.add(Bytes.toBytes("info"),
                Bytes.toBytes("address"),
                Bytes.toBytes("shanghai"));
        table.put(put);
        table.close();
    }

    public void delete() throws IOException {
        String tableName = "user"; //实际要加命名空间 default:user
        HTable table = getHTableByTableName(tableName);
        Delete delete =new Delete(Bytes.toBytes("10004"));
        //delete.deleteColumn(Bytes.toBytes("info"),Bytes.toBytes("address"));
        delete.deleteFamily(Bytes.toBytes("info"));
        table.delete(delete);
        table.close();
    }
    public static void main(String[] args) throws IOException {
        String tableName = "event_logs"; //实际要加命名空间 default:user
        HTable table = null;
        ResultScanner scanner=null;
        try {
            table = getHTableByTableName(tableName);
            Scan scan=new Scan();
            scan.setStartRow(Bytes.toBytes("1503676800000"));
            scan.setStopRow(Bytes.toBytes("1503849600000"));
            //Scan scan1=new Scan(Bytes.toBytes("10001"),Bytes.toBytes("10003"));

         //   scan.setFilter(); 过滤器
          //  scan.setCacheBlocks();
         //   scan.setCaching();
            FilterList filterList = new FilterList();
            // 过滤数据，只分析launch事件
            filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME), Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("e_l")));
            // 定义mapper中需要获取的列名
            String[] columns = new String[] { EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME, EventLogConstants.LOG_COLUMN_NAME_UUID, EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, EventLogConstants.LOG_COLUMN_NAME_PLATFORM, EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION };
            filterList.addFilter(getColumnFilter(columns));

          //  scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(EventLogConstants.HBASE_NAME_EVENT_LOGS));
            scan.setFilter(filterList);


             scanner = table.getScanner(scan);
            for (Result result : scanner) {
                System.out.println(Bytes.toString(result.getRow()));
               // System.out.println(result);
                for (Cell cell : result.rawCells()) {
                    System.out.println(
                    Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
                            + Bytes.toString(CellUtil.cloneQualifier(cell)) + "->"
                            + Bytes.toString(CellUtil.cloneValue(cell)));
                }
                System.out.println("------------------------");
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(scanner);
            IOUtils.closeStream(table);
        }


    }



    private static Filter getColumnFilter(String[] columns) {
        int length = columns.length;
        byte[][] filter = new byte[length][];
        for (int i = 0; i < length; i++) {
            filter[i] = Bytes.toBytes(columns[i]);
        }
        return new MultipleColumnPrefixFilter(filter);
    }
}
