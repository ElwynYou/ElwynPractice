package zookeeper.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Package zookeeper.lock
 * @Description:
 * @Author elwyn
 * @Date 2017/8/28 2:17
 * @Email elonyong@163.com
 */
public class CuratorLockOrder {
    static CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString("")
            .retryPolicy(new ExponentialBackoffRetry(1000, 1))
            .build();

    public static void main(String[] args) {
        client.start();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ExecutorService executorService = Executors.newCachedThreadPool();
        InterProcessMutex lock = new InterProcessMutex(client, "/zookeeper/lock");
        for (int i = 0; i < 10; i++) {
            executorService.submit(() -> {
                try {
                    countDownLatch.await();
                    lock.acquire();
                    System.out.println(Thread.currentThread().getName() + "订单:" + getOrderSNo());
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        lock.release();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        countDownLatch.countDown();
        executorService.shutdown();
    }

    static int i = 0; //利用++i操作提高操作延迟

    public static String getOrderSNo() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYYMMDDHHmmss");
        return simpleDateFormat.format(new Date()) + ++i;
    }
}
