package designMode.adapter;

/**
 * @Name designMode.adapter.Adapter
 * @Description
 * @Author Elwyn
 * @Version 2017/6/12
 * @Copyright 上海云辰信息科技有限公司
 **/
public class Adapter extends  Source implements Targetable {
	@Override
	public void method2() {
		System.out.println("this is the targetable method!");
	}
}
