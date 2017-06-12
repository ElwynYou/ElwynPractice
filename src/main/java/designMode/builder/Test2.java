package designMode.builder;

import designMode.abstractFactory.MailFactory;
import designMode.abstractFactory.Provider;
import designMode.abstractFactory.Sender;

/**
 * @Name designMode.abstractFactory.Test
 * @Description
 * @Author Elwyn
 * @Version 2017/6/12
 * @Copyright 上海云辰信息科技有限公司
 **/
public class Test2 {
	public static void main(String[] args) {
		Builder builder=new Builder();
		builder.produceMailSender(3);
		builder.produceSmsSender(1);

	}
}
