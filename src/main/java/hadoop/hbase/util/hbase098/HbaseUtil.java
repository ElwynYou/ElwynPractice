package hadoop.hbase.util.hbase098;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * @Package hadoop.hbase.util
 * @Description: //todo(用一句话描述该文件做什么)
 * @Author elwyn
 * @Date 2017/8/3 22:56
 * @Email elonyong@163.com
 */
public class HbaseUtil {
    public static Configuration getHBaseConfiguration(){
        Configuration configuration = HBaseConfiguration.create();
        //如果没有hbase-site.xml配置文件可以通过代码直接指定zookeeper的服务地址
        configuration.set("hbase.zookeeper.quorum", "hadoop-master.nebuinfo.com");
        return configuration;
    }
}
