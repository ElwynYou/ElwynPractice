package zookeeper.distlock;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Package zookeeper.distlock
 * @Description:
 * @Author elwyn
 * @Date 2017/9/4 1:57
 * @Email elonyong@163.com
 */
public class OrderNumGenerator {
    private static int i = 0;

    public  String getOrderNum() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-MM-ss-");
        return simpleDateFormat.format(new Date()) + ++i;
    }
}
