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
	private static final String CSV_DELIMITER = ",";
	private static final int SPLITS = 5;
	private static final int BLOCKS = 5000;

	private static String csvFilepath = "/media/SHARED/repositories/BigDataProject/doc/source/SET-dec-2013.csv";

	public static void main(String[] args) throws IOException {
		int f = 5;
		int c = 5;

		// Borramos todas las tablas
		dropTables();

		// Creamos la estructura de las tablas
		createTable(c);
		bootstrapping(f, c);

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

	private static void createTable(int measures) throws IOException {
		try (Connection connection = HBaseConnector.getConnection()) {
			Admin admin = connection.getAdmin();
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));

			for (int m = 1; m <= measures; m++) {
				tableDescriptor.addFamily(new HColumnDescriptor(String.format("%s%d", CF_MEASUREX, m)));
			}

			byte[][] splits = new byte[SPLITS][];
			for (int i = 0; i < SPLITS; i++) {
				splits[i] = Bytes.toBytes(Integer.toString(i + 1));
			}

			admin.createTable(tableDescriptor, splits);
		}
	}

	private static void bootstrapping(int f, int c) throws IOException {
		ArrayList<MeterReading> meterReadings = readCsv(csvFilepath, CSV_DELIMITER);

		try (Connection connection = HBaseConnector.getConnection()) {
			BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf(TABLE_NAME));
			ArrayList<Put> puts = new ArrayList<Put>();
			int nPuts = 0;

			for (MeterReading meterReading : meterReadings) {
				for (int line = 1; line <= f; line++) {
					Put put = new Put(Bytes.toBytes(GetRowKey(line, meterReading)));

					for (int col = 1; col <= c; col++) {
						put.addColumn(Bytes.toBytes(String.format("%s%d", CF_MEASUREX, col)),
								Bytes.toBytes(meterReading.getHHmm()), Bytes.toBytes(meterReading.getMeasure()));
					}

					puts.add(put); nPuts++;

					if (puts.size() % BLOCKS == 0) {
						SendAndClearBlockToHbaseTable(mutator, puts);
					}
				}
			}

			if (puts.size() != 0) {
				SendAndClearBlockToHbaseTable(mutator, puts);
			}
			
			System.out.println(String.format("Enviandos %d 'puts' al servidor", nPuts));
		}
	}

	private static void SendAndClearBlockToHbaseTable(BufferedMutator mutator, ArrayList<Put> puts) throws IOException {
		System.out.println(String.format("Enviando %d 'puts' al servidor", puts.size()));
		mutator.mutate(puts);
		mutator.flush();
		puts.clear();
		System.out.println("Enviados");
	}

	private static int computeBucket(String key, int buckets) {
		int rawHash = key.hashCode();
		int positiveHash = rawHash & Integer.MAX_VALUE;
		return positiveHash % buckets;
	}

	private static String GetRowKey(int line, MeterReading mr) {
		int bucket = computeBucket(line + mr.getSensor() + mr.getDatetime(), SPLITS);
		String rowKey = bucket + "#" + line + mr.getSensor() + "#" + mr.getDay();
		return line + mr.getSensor() + "#" + mr.getDay();//rowKey;
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
