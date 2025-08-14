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

		int f = 1;
		int c = 2;

		// Borramos todas las tablas
		dropTables();

		// Creamos la estructura de las tablas
		createTable(c);

		bootstrapping(f, c);
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

//			tableDescriptor.addFamily(new HColumnDescriptor(ROW_ID));
//			tableDescriptor.addFamily(new HColumnDescriptor(CF_DATETIME));
			for (int m = 1; m <= measures; m++) {
				tableDescriptor.addFamily(new HColumnDescriptor(String.format("%s%d", CF_MEASUREX, m)));
			}

			// Creamos la tabla
			admin.createTable(tableDescriptor);
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
					Put put = new Put(Bytes.toBytes(String.format("%d%s_%s", line, meterReading.getSensor(), meterReading.getDay())));
//					put.addColumn(Bytes.toBytes(CF_DATETIME),Bytes.toBytes(C_DATETIME_DAY)
//							, Bytes.toBytes(meterReading.getDay()));
					
					for(int col = 1; col <= c; col++) {
						put.addColumn(Bytes.toBytes(String.format("%s%d", CF_MEASUREX, col)), Bytes.toBytes(meterReading.getHHmm())
								, Bytes.toBytes(meterReading.getMeasure()));
					}
					
					puts.add(put);					
					
					if(puts.size() % 1000 == 0) {
						table.put(puts);
					}
				}
			}
			
			if(puts.size() % 1000 != 0) {
				table.put(puts);
			}

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
//				System.out.println(mr.toString());
			}

			System.out.println(meterReadings.size());
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
