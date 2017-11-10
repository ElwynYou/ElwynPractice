package spark;

import java.io.Serializable;
import java.sql.Date;

public class ScanEndingImprove implements Serializable {
		private String imsi;
		private String machineId;
		private Date captureTime;

		public ScanEndingImprove() {
		}

		public ScanEndingImprove(String imsi, String machineId, Date captureTime) {
			this.imsi = imsi;
			this.machineId = machineId;
			this.captureTime = captureTime;
		}

		public String getImsi() {
			return imsi;
		}

		public void setImsi(String imsi) {
			this.imsi = imsi;
		}

		public String getMachineId() {
			return machineId;
		}

		public void setMachineId(String machineId) {
			this.machineId = machineId;
		}

		public Date getCaptureTime() {
			return captureTime;
		}

		public void setCaptureTime(Date captureTime) {
			this.captureTime = captureTime;
		}
	}