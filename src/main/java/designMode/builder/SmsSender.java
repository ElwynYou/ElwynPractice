package designMode.builder;

/**
 * @Name designMode.abstractFactory.MailSender
 * @Description
 * @Author Elwyn
 * @Version 2017/6/12
 * @Copyright 上海云辰信息科技有限公司
 **/
public class SmsSender implements Sender {
	@Override
	public void send() {
		System.out.println("this is mail SmsSender");
	}
}
