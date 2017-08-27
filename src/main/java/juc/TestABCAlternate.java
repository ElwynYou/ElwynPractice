package juc;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Name juc.TestABCAlternate
 * @Description 开启3个线程, 这三个线程 分别A,B,C每个线程将自己的ID在屏幕上打印10遍,要求输出的结果必须按顺序执行
 * @Author Elwyn
 * @Version 2017/8/19
 * @Copyright 上海云辰信息科技有限公司
 **/
public class TestABCAlternate {
	public static void main(String[] args) {
		AlternateDemo alternateDemo = new AlternateDemo();
		new Thread(new Runnable() {
			@Override
			public void run() {
				for (int i = 0; i <= 5; i++) {
					alternateDemo.loopA(i);
				}
			}
		}, "A").start();
		new Thread(new Runnable() {
			@Override
			public void run() {
				for (int i = 0; i <= 5; i++) {
					alternateDemo.loopB(i);
				}
			}
		}, "B").start();
		new Thread(new Runnable() {
			@Override
			public void run() {
				for (int i = 0; i <= 5; i++) {
					alternateDemo.loopC(i);
					System.out.println("================================");
				}
			}
		}, "C").start();
	}
}

class AlternateDemo {
	private int number = 1;
	private Lock lock = new ReentrantLock();
	private Condition condition1 = lock.newCondition();
	private Condition condition2 = lock.newCondition();
	private Condition condition3 = lock.newCondition();

	/**
	 * @param totalLoop 循环第几伦
	 */
	public void loopA(int totalLoop) {

		lock.lock();
		try {
			if (number != 1) {
				condition1.await();
			}
			for (int i = 1; i <= 1; i++) {
				System.out.println(Thread.currentThread().getName() + "\t" + i + "\t" + totalLoop);
			}
			number = 2;
			condition2.signal();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			lock.unlock();
		}
	}

	/**
	 * @param totalLoop 循环第几伦
	 */
	public void loopB(int totalLoop) {

		lock.lock();
		try {
			if (number != 2) {
				condition2.await();
			}
			for (int i = 1; i <= 1; i++) {
				System.out.println(Thread.currentThread().getName() + "\t" + i + "\t" + totalLoop);
			}
			number = 3;
			condition3.signal();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			lock.unlock();
		}
	}

	/**
	 * @param totalLoop 循环第几伦
	 */
	public void loopC(int totalLoop) {

		lock.lock();
		try {
			if (number != 3) {
				condition3.await();
			}
			for (int i = 1; i <= 1; i++) {
				System.out.println(Thread.currentThread().getName() + "\t" + i + "\t" + totalLoop);
			}
			number = 1;
			condition1.signal();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			lock.unlock();
		}
	}

}