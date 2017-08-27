package zookeeper.lock;

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
public class NoLockOrder {

    public static void main(String[] args) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < 10; i++) {
            executorService.submit(() -> {
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(Thread.currentThread().getName() + "订单:" + getOrderSNo());
            });
        }

        countDownLatch.countDown();
        executorService.shutdown();
    }

    public static String getOrderSNo() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYYMMDDHHmmss");
        return simpleDateFormat.format(new Date());
    }
}
