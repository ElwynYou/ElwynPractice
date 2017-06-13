package designMode.proxy;

/**
 * @Name designMode.proxy.SourceImpl
 * @Description
 * @Author Elwyn
 * @Version 2017/6/13
 * @Copyright 上海云辰信息科技有限公司
 **/
public class SourceImpl implements Source {
	@Override
	public void method1() {
		System.out.println("the original method");
	}
}
