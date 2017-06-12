package designMode.abstractFactory;

/**
 * @Name designMode.abstractFactory.Test
 * @Description
 * @Author Elwyn
 * @Version 2017/6/12
 * @Copyright 上海云辰信息科技有限公司
 **/
public class Test {
	public static void main(String[] args) {
		Provider provider= new MailFactory();
		Sender sender = provider.produce();
		sender.send();
	}
}
