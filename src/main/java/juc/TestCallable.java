package juc;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * @Name juc.TestCallable
 * @Description 创建执行线程的方式:实现Callable接口
 * @Author Elwyn
 * @Version 2017/8/19
 * @Copyright 上海云辰信息科技有限公司
 **/
public class TestCallable {
	public static void main(String[] args) {
		ThreadDemo threadDemo = new ThreadDemo();
		//执行Callable方式,需要FutureTask 实现类的支持,用于接收计算结果.
		FutureTask<Integer> futureTask = new FutureTask<>(threadDemo);
		new Thread(futureTask).start();
		try {
			//接收线程运算后的结果.
			Integer integer = futureTask.get();//FutureTask 可用于闭锁
			System.out.println(integer);
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}
}

class ThreadDemo implements Callable<Integer> {
	@Override
	public Integer call() throws Exception {
		int sum = 0;
		System.out.println("当call方法执行完成后futuretask的get方法才会执行");
		for (int i = 0; i < 10; i++) {
			Thread.sleep(1000);
			sum += i;
		}
		return sum;
	}
}
