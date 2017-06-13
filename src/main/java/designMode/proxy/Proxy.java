package designMode.proxy;

/**
 * @Name designMode.proxy.Proxy
 * @Description
 * @Author Elwyn
 * @Version 2017/6/13
 * @Copyright 上海云辰信息科技有限公司
 **/
public class Proxy implements Source {
	private Source source;

	public Proxy(){
		super();
		this.source=new SourceImpl();
	}



	@Override
	public void method1() {
		before();
		source.method1();
		after();
	}


	private void before(){
		System.out.println("proxy method before");
	}
	private void after(){
		System.out.println("proxy method after");
	}
}
