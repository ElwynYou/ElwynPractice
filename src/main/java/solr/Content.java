package solr;

import java.io.Serializable;

/**
 * @Name solr.Content
 * @Description
 * @Author Elwyn
 * @Version 2017/7/19
 * @Copyright 上海云辰信息科技有限公司
 **/
public class Content implements Serializable {
	private static final long serialVersionUID = 6789275038989033472L;

	private String service_code;
	private String url;
	private String id;

	public String getService_code() {
		return service_code;
	}

	public void setService_code(String service_code) {
		this.service_code = service_code;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return "Content{" +
				"service_code='" + service_code + '\'' +
				", url='" + url + '\'' +
				", id='" + id + '\'' +
				'}';
	}
}
