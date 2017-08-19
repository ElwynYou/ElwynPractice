package juc;

import org.apache.hadoop.fs.shell.Count;

import java.util.concurrent.CountDownLatch;

/**
 * @Name juc.TestCountDownLatch
 * @Description CountDownLatch:闭锁,在完成某些运算时,只有其他所有线程的运算全部完成,当前运算才能继续执行
 * @Author Elwyn
 * @Version 2017/8/19
 * @Copyright 上海云辰信息科技有限公司
 **/
public class TestCountDownLatch {
	public static void main(String[] args) {
		final CountDownLatch countDownLatch = new CountDownLatch(5);
		LatchDemo latchDemo = new LatchDemo(countDownLatch);
		long start = System.currentTimeMillis();
		for (int i = 0; i < 5; i++) {
			new Thread(latchDemo).start();
		}
		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		System.out.println("耗费时间为:" + (end - start));

	}
}

class LatchDemo implements Runnable {
	private CountDownLatch latch;

	public LatchDemo(CountDownLatch countDownLatch) {
		this.latch = countDownLatch;
	}

	@Override
	public void run() {
		try {
			synchronized (this) {
				for (int i = 0; i < 50000; i++) {
					if (i % 2 == 0) {
						System.out.println(i);
					}
				}
			}
		}finally {

			latch.countDown();
		}

	}
}
