package parte1;

import org.apache.hadoop.hbase.util.Bytes;

public class SynteticData {
	private String sensor_;
	private byte[] bSensor_;
	private String datetime_;
	private String day_;
	private byte[] bDay_;
	private String HHmm_;
	private byte[] bHHmm_;
	private String measure_;
	private byte[] bMeasure_;


		
	public SynteticData(String sensor, String datetime, String measure) {
		setSensor(sensor);
		setDatetime(datetime);
		setMeasure(measure);
	}

	public void setSensor(String sensor) {
		sensor_ = sensor;
		bSensor_ = Bytes.toBytes(sensor_);
	}

	public void setDatetime(String datetime) {
		datetime_ = datetime;
		day_ = datetime_.split(" ")[0];
		bDay_ = Bytes.toBytes(day_);
		HHmm_ = datetime_.split(" ")[1];
		bHHmm_ = Bytes.toBytes(HHmm_);
	}

	public void setMeasure(String measure) {
		measure_ = measure;
		bMeasure_ = Bytes.toBytes(measure_);
	}

	public byte[] getSensor() {
		return bSensor_;
	}
	
	public String getSensorAsString() {
		return sensor_;
	}
	
	public String getDatetimeAsString() {
		return datetime_;
	}

	public byte[] getDay() {
		return bDay_;
	}
	
	public String getDayAsString() {
		return day_;
	}

	public byte[] getHHmm() {
		return bHHmm_;
	}

	public byte[] getMeasure() {
		return bMeasure_;
	}
		
	public String getMeasureAsString() {
		return measure_;
	}

	@Override
	public String toString() {
		return "Lectura [Sensor=" + sensor_ + ", Datetime=" + datetime_ + ", Measures=" + measure_ + "]";
	}
}
