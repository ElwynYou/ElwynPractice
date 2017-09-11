package zookeeper.lock;

import org.apache.zookeeper.KeeperException;

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
public class ZkLockOrder {
    static ZkLock zkLock = new ZkLock("", "bit");

    public static void main(String[] args) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < 10; i++) {
            executorService.submit(() -> {
                try {
                    countDownLatch.await();
                    zkLock.lock();
                    System.out.println(Thread.currentThread().getName() + "订单:" + getOrderSNo());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } finally {
                    zkLock.unLock();

                }
            });
        }

        countDownLatch.countDown();
        executorService.shutdown();
    }

   /* public static synchronized String getOrderSNo() { 双重锁没用
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYYMMDDHHmmss");
        return simpleDateFormat.format(new Date());
    }*/

    static int i = 0; //利用++i操作提高操作延迟

    public static String getOrderSNo() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYYMMDDHHmmss");
        return simpleDateFormat.format(new Date()) + ++i;
    }
}
