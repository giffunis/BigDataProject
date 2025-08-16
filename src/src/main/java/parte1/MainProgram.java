package parte1;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MainProgram {

	private static final String TABLE_NAME = "measure";
	private static final String CF_MEASUREX = "measure";
	private static final String CF_GENERAL = "general";
	private static final String CF_GENERAL_C_SENSOR = "sensor";
	private static final String CF_GENERAL_C_DAY = "day";

	private static final byte[] B_CF_GENERAL = Bytes.toBytes(CF_GENERAL);
	private static final byte[] B_CF_GENERAL_C_SENSOR = Bytes.toBytes(CF_GENERAL_C_SENSOR);
	private static final byte[] B_CF_GENERAL_C_DAY = Bytes.toBytes(CF_GENERAL_C_DAY);

	private static final String CSV_DELIMITER = ",";
	private static int N_LOCAL_REGION_SERVERS = 3;

	private static String csvFilepath = "/media/SHARED/repositories/BigDataProject/doc/source/SET-dec-2013.csv";

	public static void main(String[] args) throws IOException {
		int factorF = 5;
		int factorC = 5;

		// Borramos todas las tablas
		dropTables();

		// Creamos la estructura de la tabla
		HTableDescriptor tableDescriptor = defineTable(factorC);
		createTable(tableDescriptor);

		// Leemos el fichero y aplicamos el bootstrapping
		List<SynteticData> synteticData = generateSyntheticReadings(readCsv(csvFilepath, CSV_DELIMITER), factorF);
				
		// Insertamos en Hbase
		insertDataIntoHbase(tableDescriptor, synteticData);
		
		System.out.println("Terminada la escritura"); // scan 'measure', { COLUMNS => ['measure3'], FILTER =>
														// "PrefixFilter('3DG')"}

//		List<Result> rows = getRowsBySensorPrefix("3DG","measure3");
	}

	private static void dropTables() throws IOException {
		try (Connection connection = HBaseConnector.getConnection()) {
			Admin admin = connection.getAdmin();

			for (TableName table : admin.listTableNames()) {
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
					switch (column.getNameAsString()) {
					case CF_GENERAL:
						put.addColumn(B_CF_GENERAL, B_CF_GENERAL_C_SENSOR, data.getSensor());
						put.addColumn(B_CF_GENERAL, B_CF_GENERAL_C_DAY, data.getDay());
						break;
					default:
						put.addColumn(column.getName(), data.getHHmm(), data.getMeasure());
						break;
					}
				}

				table.put(put);
//					System.out.println(String.format("Enviando el siguiente put: %s \n al servidor", put.toString()));
			}
		}
	}

	private static int computeBucket(String key, int buckets) {
		int rawHash = key.hashCode();
		int positiveHash = rawHash & Integer.MAX_VALUE;
		return positiveHash % (buckets);
	}

	private static String GetRowKey(SynteticData mr) {
		int bucket = computeBucket(mr.getSensorAsString() + mr.getDatetimeAsString(), N_LOCAL_REGION_SERVERS);
		String rowKey = bucket + "#" + mr.getSensorAsString() + "#" + mr.getDayAsString();
		return rowKey;
	}

//	public static List<Result> getRowsBySensorPrefix(String prefix, String cfMeasure) throws IOException {
//		// 1. Comparator basado en regex: empieza por el prefijo
//        RegexStringComparator regex = new RegexStringComparator("^" + prefix + ".*");
//
//        // 2. Filtro de columna única usando CompareOp.EQUAL y el comparator anterior
//        SingleColumnValueFilter filter = new SingleColumnValueFilter(
//            B_CF_GENERAL,
//            B_CF_GENERAL_C_SENSOR,
//            CompareOp.EQUAL,
//            regex
//        );
//        filter.setFilterIfMissing(true);
//
//        // 3. Configuración del Scan
//        Scan scan = new Scan();
//        scan.addFamily(B_CF_GENERAL);
//        scan.addFamily(Bytes.toBytes(cfMeasure));
//        scan.setFilter(filter);
//
//        // 4. Ejecución y recolección de resultados
//        List<Result> rows = new ArrayList<>();
//        try (Connection conn = HBaseConnector.getConnection();
//             Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
//             ResultScanner scanner = table.getScanner(scan)) {
//
//            for (Result r : scanner) {
//                rows.add(r);
//            }
//        }
//
//        return rows;
//    }

//	public MeterReading mapResultToPrint(Result result, String cfMeasure) {
//	    String rowKey    = Bytes.toString(result.getRow());
//	    String sensorId  = Bytes.toString(
//	        result.getValue(B_CF_GENERAL, B_CF_GENERAL_C_SENSOR)
//	    );
//	    String day       = Bytes.toString(
//	        result.getValue(B_CF_GENERAL, B_CF_GENERAL_C_DAY)
//	    );
//	    
//	    result.getFamilyMap(B_CF_GENERAL);
//	    result.getFamilyMap(Bytes.toBytes(cfMeasure));
//
//	    // Supongamos que guardas las medidas en un Map<String,Double>
//	    Map<String, Double> measures = new HashMap<>();
//	    for (byte[] family : result.getMap().keySet()) {
//	        String cf = Bytes.toString(family);
//	        
//	        if (cf.startsWith("measure")) {
//	            for (Cell cell : result.getColumnCells(family, Bytes.toBytes("00:00"))) {
//	                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
//	                double value      = Bytes.toDouble(CellUtil.cloneValue(cell));
//	                measures.put(cf + ":" + qualifier, value);
//	            }
//	        }
//	    }
//
//	    return new MeterReading(rowKey, sensorId, day, measures);
//	}
//	
	private static List<SynteticData> generateSyntheticReadings(List<OriginalData> originalData, int factorF) {
        List<SynteticData> synteticData = new ArrayList<SynteticData>();
    	
        for (OriginalData original : originalData) {	
        		
        		for (int f = 1; f <= factorF; f++) {	
        			synteticData.add(new SynteticData(String.format("%d%s", f, original.getSensor()), original.getDatetime(), original.getMeasure()));
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

}
