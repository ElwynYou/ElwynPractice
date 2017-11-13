package spark;

import java.io.Serializable;
import java.util.Date;


/**
 * 
 * @Name com.rainsoft.data.gateway.fout.entity.ScanEndingImprove
 * @Description 
 * @Author xa
 * @Version 2017年8月15日 上午10:26:59
 * @Copyright 上海云辰信息科技有限公司
 */
public class ScanEndingImprove implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private Long id;//序列ID
	private Date captureTime;//抓取时间
	private String longitude;//经度
	private String latitude;//纬度
	private Date firstTime;//首次采集时间
	private Date lastTime;//最后采集时间
	private String areaCode;//归属地编码
	private String moduleMac;//模块mac
	private String imsi;//终端IMSI
	private String isp;//手机号运营商
	private String distance;//估算距离
	private String phone;//手机号码前7位
	private Date createTime;//数据插入时间
	private String snCode;//模块编号
	private Long machineId;//设备ID
	private String manufacturerCode;//厂商编码

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Date getCaptureTime() {
		return captureTime;
	}

	public void setCaptureTime(Date captureTime) {
		this.captureTime = captureTime;
	}

	public String getLongitude() {
		return longitude;
	}

	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}

	public String getLatitude() {
		return latitude;
	}

	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}

	public Date getFirstTime() {
		return firstTime;
	}

	public void setFirstTime(Date firstTime) {
		this.firstTime = firstTime;
	}

	public Date getLastTime() {
		return lastTime;
	}

	public void setLastTime(Date lastTime) {
		this.lastTime = lastTime;
	}

	public String getAreaCode() {
		return areaCode;
	}

	public void setAreaCode(String areaCode) {
		this.areaCode = areaCode;
	}

	public String getModuleMac() {
		return moduleMac;
	}

	public void setModuleMac(String moduleMac) {
		this.moduleMac = moduleMac;
	}

	public String getImsi() {
		return imsi;
	}

	public void setImsi(String imsi) {
		this.imsi = imsi;
	}

	public String getIsp() {
		return isp;
	}

	public void setIsp(String isp) {
		this.isp = isp;
	}

	public String getDistance() {
		return distance;
	}

	public void setDistance(String distance) {
		this.distance = distance;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public String getSnCode() {
		return snCode;
	}

	public void setSnCode(String snCode) {
		this.snCode = snCode;
	}

	public Long getMachineId() {
		return machineId;
	}

	public void setMachineId(Long machineId) {
		this.machineId = machineId;
	}

	public String getManufacturerCode() {
		return manufacturerCode;
	}

	public void setManufacturerCode(String manufacturerCode) {
		this.manufacturerCode = manufacturerCode;
	}
}
