package designMode.decorator;

/**
 * @Name designMode.decorator.Source
 * @Description
 * @Author Elwyn
 * @Version 2017/6/13
 * @Copyright 上海云辰信息科技有限公司
 **/
public class Source implements Sourceable {
	@Override
	public void method() {
		System.out.println("the original method!");
	}
}
