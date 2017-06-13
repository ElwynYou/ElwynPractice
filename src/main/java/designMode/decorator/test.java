package designMode.decorator;

/**
 * @Name designMode.decorator.test
 * @Description
 * @Author Elwyn
 * @Version 2017/6/13
 * @Copyright 上海云辰信息科技有限公司
 **/
public class test {
	public static void main(String[] args) {
		Sourceable source = new Source();
		Sourceable obj = new Decorator(source);
		obj.method();
	}
}
