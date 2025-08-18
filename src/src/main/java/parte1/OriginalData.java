package parte1;

public class OriginalData {
	private String sensor;
	private String datetime;
	private String measure;
	
	public OriginalData(String sensor, String datetime, String measure) {
		super();
		this.sensor = sensor;
		this.datetime = datetime;
		this.measure = measure;
	}

	public String getSensor() {
		return sensor;
	}

	public void setSensor(String sensor) {
		this.sensor = sensor;
	}

	public String getDatetime() {
		return datetime;
	}

	public void setDatetime(String datetime) {
		this.datetime = datetime;
	}

	public String getMeasure() {
		return measure;
	}

	public void setMeasure(String measure) {
		this.measure = measure;
	}
}