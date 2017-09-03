package zookeeper.distlock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * @Package zookeeper.distlock
 * @Description:
 * @Author elwyn
 * @Date 2017/9/4 1:59
 * @Email elonyong@163.com
 */
public class OrderService implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(OrderService.class);
    private OrderNumGenerator orderNumGenerator = new OrderNumGenerator();
    private static CountDownLatch countDownLatch = new CountDownLatch(100);
    private Lock lock = new ZookeeperDistributeLock();

    @Override
    public void run() {
        createOrderRemote();
    }

    private void createOrderRemote() {
        lock.getLock();
        String orderNum = orderNumGenerator.getOrderNum();
        logger.info(Thread.currentThread().getName() + "得到订单号:" + orderNum);
        lock.unLock();
    }

    public void createOrderLocal() {
        String orderNum = orderNumGenerator.getOrderNum();
        logger.info(Thread.currentThread().getName() + "得到订单号:" + orderNum);
    }

    public static void main(String[] args) {
        for (int i = 0; i < 100; i++) {
            new Thread(new OrderService()).start();
            countDownLatch.countDown();
        }
    }
}
