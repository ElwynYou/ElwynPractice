package spark;

import java.io.Serializable;

/**
 * @Name spark.ServiceInfo
 * @Description
 * @Author Elwyn
 * @Version 2017/11/9
 * @Copyright 上海云辰信息科技有限公司
 **/
public class ServiceInfo implements Serializable {
	private String serviceCode="";
	private String serviceName="";
	private String serviceType="";

	public ServiceInfo() {
	}

	public ServiceInfo(String serviceCode, String serviceName, String serviceType) {
		this.serviceCode = serviceCode;
		this.serviceName = serviceName;
		this.serviceType = serviceType;
	}

	public String getServiceCode() {
		return serviceCode;
	}

	public void setServiceCode(String serviceCode) {
		this.serviceCode = serviceCode;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public String getServiceType() {
		return serviceType;
	}

	public void setServiceType(String serviceType) {
		this.serviceType = serviceType;
	}

	@Override
	public String toString() {
		return "ServiceInfo{" +
				"serviceCode='" + serviceCode + '\'' +
				", serviceName='" + serviceName + '\'' +
				", serviceType='" + serviceType + '\'' +
				'}';
	}
}
