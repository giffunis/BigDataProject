package parte1;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

public class MainProgram {

	private static final String TABLE_NAME = "measure";
	private static final String CF_MEASUREX = "measure";
	private static final String CF_GENERAL = "general";
	private static final String CF_GENERAL_C_SENSOR = "sensor";
	private static final String CF_GENERAL_C_DATE = "day";

	private static final byte[] B_CF_GENERAL = Bytes.toBytes(CF_GENERAL);
	private static final byte[] B_CF_GENERAL_C_SENSOR = Bytes.toBytes(CF_GENERAL_C_SENSOR);
	private static final byte[] B_CF_GENERAL_C_DATE = Bytes.toBytes(CF_GENERAL_C_DATE);

	private static final String CSV_DELIMITER = ",";
	private static int N_LOCAL_REGION_SERVERS = 3;

	private static String inputCsvFilepath = "/media/SHARED/repositories/BigDataProject/doc/source/SET-dec-2013.csv";
	private static String outputCsvFilepath = "/media/SHARED/repositories/BigDataProject/doc/source/output.csv";

	public static void main(String[] args) throws IOException {

		int factorF = 5;
		int factorC = 5;
		int extF = 3;
		int extC = 3;
		
		// Borramos todas las tablas
		dropTables();
		
		// Creamos la estructura de la tabla
		HTableDescriptor tableDescriptor = defineTable(factorC);
		createTable(tableDescriptor);
		
		// Leemos el fichero y aplicamos el bootstrapping
		List<SynteticData> synteticData = generateSyntheticReadings(readCsv(inputCsvFilepath, CSV_DELIMITER),factorF);
		
		// Insertamos en Hbase 
		insertDataIntoHbase(tableDescriptor, synteticData);
		System.out.println("Terminada la escritura");

		String cF = String.format("%s%d", CF_MEASUREX, extC);
		String fId = String.format("%dDG", extF);
		
		writeCsv(outputCsvFilepath, generateHeader(), getRowsBySensorPrefix(fId, cF));

	}

	private static void dropTables() throws IOException {
		try (Connection connection = HBaseConnector.getConnection()) {
			Admin admin = connection.getAdmin();

			for (TableName table : admin.listTableNames()) {
				if(!admin.isTableDisabled(table))
					admin.disableTable(table);
				admin.deleteTable(table);
			}
		}
	}

	private static HTableDescriptor defineTable(int nMeasuresBySensor) {
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));

		tableDescriptor.addFamily(new HColumnDescriptor(CF_GENERAL));

		for (int m = 1; m <= nMeasuresBySensor; m++) {
			tableDescriptor.addFamily(new HColumnDescriptor(String.format("%s%d", CF_MEASUREX, m)));
		}

		return tableDescriptor;
	}

	private static void createTable(HTableDescriptor tableDescriptor) throws IOException {
		try (Connection connection = HBaseConnector.getConnection()) {
			Admin admin = connection.getAdmin();

			byte[][] splits = new byte[N_LOCAL_REGION_SERVERS - 1][];
			for (int i = 1; i < N_LOCAL_REGION_SERVERS; i++) {
				splits[i - 1] = Bytes.toBytes(Integer.toString(i));
			}

			admin.createTable(tableDescriptor, splits);
		}
	}

	private static void insertDataIntoHbase(HTableDescriptor tableDescriptor, List<SynteticData> synteticData)
			throws IOException {
		HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();

		try (Connection connection = HBaseConnector.getConnection()) {
			Table table = connection.getTable(tableDescriptor.getTableName());

			for (SynteticData data : synteticData) {

				Put put = new Put(Bytes.toBytes(GetRowKey(data)));

				for (HColumnDescriptor column : columnFamilies) {
					String name = column.getNameAsString();
					switch (name) {
					case CF_GENERAL:
						put.addColumn(B_CF_GENERAL, B_CF_GENERAL_C_SENSOR, data.getSensor());
						put.addColumn(B_CF_GENERAL, B_CF_GENERAL_C_DATE, data.getDay());
						break;
					default:
						put.addColumn(column.getName(), data.getHHmm(), data.getMeasure());
						break;
					}
				}

				table.put(put);
			}
		}
	}

	private static int computeBucket(String key, int buckets) {
		int rawHash = key.hashCode();
		int positiveHash = rawHash & Integer.MAX_VALUE;
		return positiveHash % (buckets);
	}

	private static String GetRowKey(SynteticData mr) {
		int bucket = computeBucket(mr.getSensorAsString() + mr.getDayAsString(), N_LOCAL_REGION_SERVERS);
		String rowKey = bucket + "#" + mr.getSensorAsString() + "#" + mr.getDayAsString();
		return rowKey;
	}

	public static List<Result> getRowsBySensorPrefix(String prefix, String cfMeasure) throws IOException {

		RegexStringComparator regex = new RegexStringComparator("^" + prefix + ".*");

		// Filtro
		SingleColumnValueFilter filter = new SingleColumnValueFilter(B_CF_GENERAL, B_CF_GENERAL_C_SENSOR,
				CompareOp.EQUAL, regex);
		filter.setFilterIfMissing(true);

		// Configuración del Scan
		Scan scan = new Scan();
		scan.addFamily(B_CF_GENERAL);
		scan.addFamily(Bytes.toBytes(cfMeasure));
		scan.setFilter(filter);

		// Ejecución
		List<Result> rows = new ArrayList<>();
		try (Connection conn = HBaseConnector.getConnection();
				Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
				ResultScanner scanner = table.getScanner(scan)) {

			for (Result r : scanner) {
				rows.add(r);
			}
		}
		
		// Ordenar por sensor (alfabéticamente) y luego por day (numéricamente)
		rows.sort(Comparator
		    .comparing((Result r) -> {
		        byte[] sensorBytes = r.getValue(B_CF_GENERAL, B_CF_GENERAL_C_SENSOR);
		        return Bytes.toString(sensorBytes);
		    })
		    .thenComparing((Result r) -> {
		        byte[] dayBytes = r.getValue(B_CF_GENERAL, B_CF_GENERAL_C_DATE);
		        return Bytes.toString(dayBytes);
		    })
		);

		System.out.println(String.format("Se han recuperado %d filas", rows.size()));
		return rows;
	}

	public static void writeCsv(String filePath, List<String> header, List<Result> results)
			throws IOException {
		try (FileWriter fw = new FileWriter(filePath)) {

	        fw.write(String.join(CSV_DELIMITER, header));

			for (Result result : results) {
				fw.write("\n");
				fw.write(String.join(CSV_DELIMITER, mapResult(result)));
			}
		}
		System.out.println(String.format("Se han escrito %d filas, más la cabecera.", results.size()));
	}

	public static List<String> mapResult(Result result) {
		List<String> line = new ArrayList<String>();
		line.add(Bytes.toString(result.getValue(B_CF_GENERAL, B_CF_GENERAL_C_SENSOR)).substring(1)); // Eliminamos el primer char
		line.add(Bytes.toString(result.getValue(B_CF_GENERAL, B_CF_GENERAL_C_DATE)));

	
		for (Cell cell : result.listCells()) {
	        String cf = Bytes.toString(
	            cell.getFamilyArray(),
	            cell.getFamilyOffset(),
	            cell.getFamilyLength()
	        );
	        
	        if(cf.contains(CF_MEASUREX)) {
	        	// Obtener el mapa de columnas dentro de la familia 'measureX'
	    		NavigableMap<byte[], byte[]> columnas = result.getFamilyMap(Bytes.toBytes(cf));

	    		for (Entry<byte[], byte[]> entry : columnas.entrySet()) {
	    			line.add(Bytes.toString(entry.getValue()));
	    		}
	    		// Salimos del bucle porque no nos interesan las demás columnas
	    		break;
	        }
	    }

		return line;
	}

	private static List<SynteticData> generateSyntheticReadings(List<OriginalData> originalData, int factorF) {
		List<SynteticData> synteticData = new ArrayList<SynteticData>();

		for (OriginalData original : originalData) {

			for (int f = 1; f <= factorF; f++) {
				synteticData.add(new SynteticData(String.format("%d%s", f, original.getSensor()),
						original.getDatetime(), original.getMeasure()));
			}
		}

		System.out.println(String.format("Se han generado %d filas", synteticData.size()));
		return synteticData;
	}

	private static List<OriginalData> readCsv(String csvFilePath, String csvDelimiter) {
		List<OriginalData> meterReadings = new ArrayList<OriginalData>();

		try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(csvDelimiter);
				OriginalData mr = new OriginalData(values[0], values[1], values[2]);
				meterReadings.add(mr);
			}

		} catch (FileNotFoundException ex) {
			System.err.println(ex.getMessage());
		} catch (IOException ex) {
			System.err.println(ex.getMessage());
		}

		System.out.println(String.format("Se han leido %d filas del fichero", meterReadings.size()));

		return meterReadings;
	}
	
	private static List<String> generateHeader() {
		List<String> header = new ArrayList<String>();
		header.add("Sensor"); header.add("Date"); header.addAll(generateTimeIntervals());
		return header;
	}
	
	private static List<String> generateTimeIntervals () {
    	List<String> timeIntervals = new ArrayList<String>();
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("HH:mm");
        for (int hour = 0; hour < 24; hour++) {
            for (int minute = 0; minute < 60; minute += 10) {
                LocalTime time = LocalTime.of(hour, minute);
                timeIntervals.add(time.format(fmt));
            }
        }
        return timeIntervals;
    }

}
