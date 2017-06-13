package designMode.proxy;

/**
 * @Name designMode.proxy.test1
 * @Description
 * @Author Elwyn
 * @Version 2017/6/13
 * @Copyright 上海云辰信息科技有限公司
 **/
public class test1 {

	public static void main(String[] args) {
		Source source=new Proxy();
		source.method1();
	}
}
