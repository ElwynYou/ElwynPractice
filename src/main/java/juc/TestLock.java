package juc;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Name juc.TestLock
 * @Description 用于解决多线程安全问题的方式:
 * 1.同步代码块
 * <p>
 * 2.同步方法
 * <p>
 * jdk1.5后
 * 3.同步锁Lock
 * 是一个显示锁:需要通过lock()上锁,必须通过unlock()释放锁
 * @Author Elwyn
 * @Version 2017/8/19
 * @Copyright 上海云辰信息科技有限公司
 **/
public class TestLock {
	public static void main(String[] args) {
		Ticket ticket = new Ticket();
		new Thread(ticket, "1号窗口").start();
		new Thread(ticket, "2号窗口").start();
		new Thread(ticket, "3号窗口").start();
	}
}

class Ticket implements Runnable {
	private int tick = 100;
	private Lock lock = new ReentrantLock();

	@Override
	public void run() {
		while (true) {
			lock.lock();//上锁
			if (tick > 0) {
				try {
					Thread.sleep(200);
					System.out.println(Thread.currentThread().getName() + "完成售票:余票为:" + --tick);
				} catch (InterruptedException e) {
					e.printStackTrace();
				} finally {
					lock.unlock();//释放锁
				}
			}
		}
	}
}