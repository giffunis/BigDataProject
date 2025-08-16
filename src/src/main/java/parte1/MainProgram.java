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

public class MainProgram {

	private static final String TABLE_NAME = "measure";
	private static final String CF_MEASUREX = "measure";
	private static final String CF_GENERAL = "general";
	private static final String CF_GENERAL_C_SENSOR = "sensor";
	private static final String CF_GENERAL_C_DAY = "day";

	private static byte[] B_CF_GENERAL = Bytes.toBytes(CF_GENERAL);
	private static byte[] B_CF_GENERAL_C_SENSOR = Bytes.toBytes(CF_GENERAL_C_SENSOR);
	private static byte[] B_CF_GENERAL_C_DAY = Bytes.toBytes(CF_GENERAL_C_DAY);

	private static final String CSV_DELIMITER = ",";
	private static final int N_LOCAL_REGION_SERVERS = 3;

	private static String csvFilepath = "/media/SHARED/repositories/BigDataProject/doc/source/SET-dec-2013.csv";

	public static void main(String[] args) throws IOException {
		int f = 1;
		int c = 1;

		// Borramos todas las tablas
		dropTables();

		// Creamos la estructura de la tabla
		HTableDescriptor tableDescriptor = defineTable(c);
		createTable(tableDescriptor);

		bootstrapping(tableDescriptor,f, c);

		System.out.println("Terminada la escritura"); // scan 'measure', { COLUMNS => ['measure3'], FILTER =>
														// "PrefixFilter('3DG')"}
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

			byte[][] splits = new byte[N_LOCAL_REGION_SERVERS][];
			for (int i = 0; i < N_LOCAL_REGION_SERVERS; i++) {
				splits[i] = Bytes.toBytes(Integer.toString(i + 1));
			}

			admin.createTable(tableDescriptor, splits);
		}
	}

	private static void bootstrapping(HTableDescriptor tableDescriptor, int f, int c) throws IOException {
		ArrayList<MeterReading> meterReadings = readCsv(csvFilepath, CSV_DELIMITER);
		HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();

		try (Connection connection = HBaseConnector.getConnection()) {
			Table table = connection.getTable(tableDescriptor.getTableName());

			for (MeterReading meterReading : meterReadings) {
				for (int line = 1; line <= f; line++) {

					Put put = new Put(Bytes.toBytes(GetRowKey(line, meterReading)));

					for (HColumnDescriptor column : columnFamilies) {
						switch (column.getNameAsString()) {
						case CF_GENERAL:
							put.addColumn(B_CF_GENERAL, B_CF_GENERAL_C_SENSOR, Bytes.toBytes(meterReading.getSensor()));
							put.addColumn(B_CF_GENERAL, B_CF_GENERAL_C_DAY, Bytes.toBytes(meterReading.getDay()));
							break;
						default:
							put.addColumn(column.getName(), Bytes.toBytes(meterReading.getHHmm()),
									Bytes.toBytes(meterReading.getMeasure()));
							break;
						}
					}

					table.put(put);
					System.out.println(String.format("Enviando el siguiente put: %s \n al servidor", put.toString()));
				}
			}

		}
	}

	private static int computeBucket(String key, int buckets) {
		int rawHash = key.hashCode();
		int positiveHash = rawHash & Integer.MAX_VALUE;
		return positiveHash % (buckets);
	}

	private static String GetRowKey(int line, MeterReading mr) {
		int bucket = computeBucket(line + mr.getSensor() + mr.getDatetime(), N_LOCAL_REGION_SERVERS);
		String rowKey = bucket + "#" + line + mr.getSensor() + "#" + mr.getDay();
		return rowKey;
	}

//	private static ArrayList<MeterReading> readFromHbase(int readC, int readF) throws IOException {
//		try (Connection connection = HBaseConnector.getConnection()) {
//			Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
//
//			Scan scan = new Scan(Bytes.toBytes(String.format("%dDG", readF)));
//			scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("givenName"));
//			scan.addFamily(Bytes.toBytes(String.format("%s%d", CF_MEASUREX, readC)));
//			ResultScanner scanner = table.getScanner(scan);
//			for (Result result : scanner) {
//			    result.
//			}
//		}
//	}

	private static ArrayList<MeterReading> readCsv(String csvFilePath, String csvDelimiter) {
		ArrayList<MeterReading> meterReadings = new ArrayList<MeterReading>();

		try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(csvDelimiter);
				MeterReading mr = new MeterReading(values[0], values[1], values[2]);
				meterReadings.add(mr);
			}

			System.out.println(String.format("Se han leido %d filas del fichero", meterReadings.size()));
		} catch (FileNotFoundException ex) {
			System.err.println(ex.getMessage());
		} catch (IOException ex) {
			System.err.println(ex.getMessage());
		}

		return meterReadings;
	}

}
