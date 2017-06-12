package designMode.adapter;

/**
 * @Name designMode.adapter.Wapper
 * @Description
 * @Author Elwyn
 * @Version 2017/6/12
 * @Copyright 上海云辰信息科技有限公司
 **/
public class Wapper implements Targetable {
	private Source source;

	public Wapper(Source source){
		super();
		this.source=source;
	}

	@Override
	public void method1() {
		source.method1();
	}

	@Override
	public void method2() {
		System.out.println("this is the targetable method");
	}
}
