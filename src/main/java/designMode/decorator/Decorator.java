package designMode.decorator;

/**
 * @Name designMode.decorator.Decorator
 * @Description
 * @Author Elwyn
 * @Version 2017/6/13
 * @Copyright 上海云辰信息科技有限公司
 **/
public class Decorator implements Sourceable {
	private Sourceable source;

	public Decorator(Sourceable source){
		super();
		this.source = source;
	}
	@Override
	public void method() {
		System.out.println("before decorator!");
		source.method();
		System.out.println("after decorator!");
	}
}
