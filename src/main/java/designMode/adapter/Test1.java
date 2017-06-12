package designMode.adapter;

import designMode.adapter.interfaceAdapter.SourceSub1;
import designMode.adapter.interfaceAdapter.SourceSub2;
import designMode.adapter.interfaceAdapter.Sourceable;
import org.junit.Test;

/**
 * @Name designMode.adapter.Test
 * @Description
 * @Author Elwyn
 * @Version 2017/6/12
 * @Copyright 上海云辰信息科技有限公司
 **/

public class Test1 {
	/**
	 * 测试类的适配器模式
	 *
	 */
	@Test
	public void testClass() {
		Targetable target = new Adapter();
		target.method1();
		target.method2();
	}

	/**
	 * 测试对象适配器
	 */
	@Test
	public void testObject() {
		Source source = new Source();
		Targetable targetable = new Wapper(source);
		targetable.method1();
		targetable.method2();
	}

	/**
	 * 测试接口适配器
	 */
	@Test
	public void testInterface() {
		Sourceable source1 = new SourceSub1();
		Sourceable source2 = new SourceSub2();

		source1.method1();
		source1.method2();
		source2.method1();
		source2.method2();
	}
}
