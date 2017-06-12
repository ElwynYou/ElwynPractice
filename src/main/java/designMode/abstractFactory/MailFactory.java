package designMode.abstractFactory;

/**
 * @Name designMode.abstractFactory.MailFactory
 * @Description
 * @Author Elwyn
 * @Version 2017/6/12
 * @Copyright 上海云辰信息科技有限公司
 **/
public class MailFactory implements Provider {
	@Override
	public Sender produce() {
		return new MailSender();
	}
}
