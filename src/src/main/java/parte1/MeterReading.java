package parte1;

import org.apache.hadoop.hbase.util.Bytes;

public class MeterReading {

	private String sensor_;
	private String datetime_;
	private String day_;
	private String HHmm_;
	private String measure_;
	private byte[] bDay_;
	private byte[] bHHmm_;
	private byte[] bMeasure_;

	private static final String EMPTY = "";

	public MeterReading() {
		sensor_ = EMPTY;
		datetime_ = EMPTY;
		measure_ = EMPTY;
	}

	public MeterReading(String sensor, String datetime, String measure) {
		super();
		setSensor(sensor);
		setDatetime(datetime);
		setMeasure(measure);
	}

	public void setSensor(String sensor) {
		sensor_ = sensor;
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

	public String getSensorAsString() {
		return sensor_;
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

	public String getDatetimeAsString() {
		return datetime_;
	}

	public byte[] getMeasure() {
		return bMeasure_;
	}

	@Override
	public String toString() {
		return "Lectura [Sensor=" + sensor_ + ", Datetime=" + datetime_ + ", Measure=" + measure_ + "]";
	}

}
