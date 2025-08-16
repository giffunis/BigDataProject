package parte1;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

public class SynteticData {
	private String sensor_;
	private byte[] bSensor_;
	private String datetime_;
	private String day_;
	private byte[] bDay_;
	private String HHmm_;
	private byte[] bHHmm_;
	private List<String> measures_;
	private List<byte[]> bMeasures_;


		
	public SynteticData(String sensor, String datetime, List<String> measures) {
		setSensor(sensor);
		setDatetime(datetime);
		setMeasures(measures);
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

	public void setMeasures(List<String> measures) {
		measures_ = new ArrayList<String>(measures);
		bMeasures_ = new ArrayList<byte[]>();
		
		for (String m : measures) {
			bMeasures_.add(Bytes.toBytes(m));
		}
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

	public List<byte[]> getMeasures() {
		return bMeasures_;
	}
	
	public byte[] getMeasure(int n) {
		return bMeasures_.get(n);
	}
	
	public List<String> getMeasuresAsStringList() {
		return measures_;
	}

	@Override
	public String toString() {
		return "Lectura [Sensor=" + sensor_ + ", Datetime=" + datetime_ + ", Measures=" + measures_.toString() + "]";
	}
}
