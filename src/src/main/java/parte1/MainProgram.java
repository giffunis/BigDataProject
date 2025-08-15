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
	private static final String ROW_ID = "sensorId";
	private static final String CF_DATETIME = "datetime";
	private static final String C_DATETIME_DAY = "day";
	private static final String CF_MEASUREX = "measure";

	private static String csvFilepath = "/media/SHARED/repositories/BigDataProject/doc/source/SET-dec-2013.csv";
	private static String csvDelimiter = ",";

	public static void main(String[] args) throws IOException {

		int f = 5;
		int c = 5;

		// Borramos todas las tablas
		dropTables();

		// Creamos la estructura de las tablas
		createTable(c);

		bootstrapping2(f, c);

		System.out.println("Terminada la escritura"); //scan 'measure', { COLUMNS => ['measure3'], FILTER  => "PrefixFilter('3DG')"}

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

	private static void createTable(int measures) throws IOException {

		try (Connection connection = HBaseConnector.getConnection()) {
			Admin admin = connection.getAdmin();
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));

			for (int m = 1; m <= measures; m++) {
				tableDescriptor.addFamily(new HColumnDescriptor(String.format("%s%d", CF_MEASUREX, m)));
			}

			// 5. Prepara los splits: de "0" a "9"
			byte[][] splits = new byte[3][];
			for (int i = 0; i < 3; i++) {
				splits[i] = Bytes.toBytes(Integer.toString(i));
			}

			// 6. Crea la tabla con splits si no existe
			admin.createTable(tableDescriptor, splits);

//			admin.createTable(tableDescriptor);
		}

	}

	/***
	 * 
	 * @param f Factor de repetición de las filas
	 * @param c Factor de repetición de las columnas
	 */
	private static void bootstrapping(int f, int c) throws IOException {
		ArrayList<MeterReading> meterReadings = readCsv(csvFilepath, csvDelimiter);

		try (Connection connection = HBaseConnector.getConnection()) {

			Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
			ArrayList<Put> puts = new ArrayList<Put>();

			for (MeterReading meterReading : meterReadings) {
				for (int line = 1; line <= f; line++) {

					Put put = new Put(Bytes
							.toBytes(String.format("%d%s_%s", line, meterReading.getSensor(), meterReading.getDay())));

					for (int col = 1; col <= c; col++) {
						put.addColumn(Bytes.toBytes(String.format("%s%d", CF_MEASUREX, col)),
								Bytes.toBytes(meterReading.getHHmm()), Bytes.toBytes(meterReading.getMeasure()));
					}

					puts.add(put);

					if (puts.size() % 1000 == 0) {
						table.put(puts);
						puts.clear();
					}
				}
			}

			if (puts.size() % 1000 != 0) {
				table.put(puts);
				puts.clear();
			}

		}

	}

	private static void bootstrapping2(int f, int c) throws IOException {
		ArrayList<MeterReading> meterReadings = readCsv(csvFilepath, csvDelimiter);

		try (Connection connection = HBaseConnector.getConnection()) {

			BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf(TABLE_NAME));
			ArrayList<Put> puts = new ArrayList<Put>();

			for (MeterReading meterReading : meterReadings) {
				for (int line = 1; line <= f; line++) {

					Put put = new Put(Bytes
							.toBytes(GetRowKey(line, meterReading)));

					for (int col = 1; col <= c; col++) {
						put.addColumn(Bytes.toBytes(String.format("%s%d", CF_MEASUREX, col)),
								Bytes.toBytes(meterReading.getHHmm()), Bytes.toBytes(meterReading.getMeasure()));
					}

					puts.add(put);

					if (puts.size() % 10000 == 0) {
						System.out.println("Enviando 1000 rows al servidor");
						mutator.mutate(puts);
					    mutator.flush();
						puts.clear();
						System.out.println("Enviadas");
					}
				}
			}

			if (puts.size() % 10000 != 0) {
				System.out.println("Enviando las últimas rows al servidor");
				mutator.mutate(puts);
			    mutator.flush();
				puts.clear();
				System.out.println("Enviadas");
			}
			}
	}
	
	/**
	 * Devuelve un bucket válido [0, buckets-1] usando hashCode().
	 */
	public static int computeBucket(String key, int buckets) {
	    // Asegura que el hash sea positivo
	    int rawHash = key.hashCode();
	    int positiveHash = rawHash & Integer.MAX_VALUE;
	    return positiveHash % buckets;
	}
	
	private static String GetRowKey(int line, MeterReading mr) {
		// Uso
		int N = 3; // número de buckets
		int bucket = computeBucket(line + mr.getSensor() + mr.getDatetime(), N); // 0..9
		String rowKey = bucket + "#" + line + mr.getSensor() + "#" +mr.getDay();
		return rowKey;
	}

	public static /* ArrayList<MeterReading> */ void readFromHbase(int readC, int readF) throws IOException {
		try (Connection connection = HBaseConnector.getConnection()) {
			Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

			Scan scan = new Scan(Bytes.toBytes(String.format("%dDG", readF)));
			scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("givenName"));
			scan.addFamily(Bytes.toBytes(String.format("%s%d", CF_MEASUREX, readC)));
			ResultScanner scanner = table.getScanner(scan);
//			for (Result result : scanner) {
//			    result.
//			}
		}
	}

	public static ArrayList<MeterReading> readCsv(String csvFilePath, String csvDelimiter) {
		String line; // To hold each valid data line.
		ArrayList<MeterReading> meterReadings = new ArrayList<MeterReading>();

		try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {

			while ((line = br.readLine()) != null) {
				String[] values = line.split(csvDelimiter);
				MeterReading mr = new MeterReading(values[0], values[1], values[2]);
				meterReadings.add(mr);
			}

			System.out.println(String.format("Se han leido %d filas del fichero", meterReadings.size()));
		}
		// Trap these Exceptions
		catch (FileNotFoundException ex) {
			System.err.println(ex.getMessage());
		} catch (IOException ex) {
			System.err.println(ex.getMessage());
		}

		return meterReadings;
	}

}
