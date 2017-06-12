package designMode.builder;

import java.util.ArrayList;
import java.util.List;

/**
 * @Name designMode.builder.Builder
 * @Description
 * @Author Elwyn
 * @Version 2017/6/12
 * @Copyright 上海云辰信息科技有限公司
 **/
public class Builder {
	private List<Sender> list=new ArrayList<Sender>();

	public void produceMailSender(int count){
		for (int i = 0; i < count; i++) {
			list.add(new MailSender());
		}

	}

	public void produceSmsSender(int count){
		for(int i=0; i<count; i++){
			list.add(new SmsSender());
		}
	}
}
