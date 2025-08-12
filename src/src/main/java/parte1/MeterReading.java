package parte1;

public class MeterReading {
	
	private String Sensor;
	private String Datetime;
	private String Measure;
	
	private static final String EMPTY = "";
	
	public MeterReading() {
		Sensor = EMPTY;
		Datetime = EMPTY;
		Measure = EMPTY;
	}

	public MeterReading(String sensor, String datetime, String measure) {
		super();
		Sensor = sensor;
		Datetime = datetime;
		Measure = measure;
	}

	public String getSensor() {
		return Sensor;
	}

	public void setSensor(String sensor) {
		Sensor = sensor;
	}

	public String getDatetime() {
		return Datetime;
	}

	public void setDatetime(String datetime) {
		Datetime = datetime;
	}

	public String getMeasure() {
		return Measure;
	}

	public void setMeasure(String measure) {
		Measure = measure;
	}
	
	@Override
	public String toString() {
		return "Lectura [Sensor=" + Sensor + ", Datetime=" + Datetime + ", Measure=" + Measure + "]";
	}
	
}
