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

	private static String csvFilepath = "/media/SHARED/repositories/BigDataProject/doc/source/SET-dec-2013.csv";
	private static String csvDelimiter = ",";
    
    public static void main(String[] args) throws IOException {
//        try (Connection connection = HBaseConnector.getConnection()) {
//            Table table = connection.getTable(TableName.valueOf("demo"));
//            Get get = new Get(Bytes.toBytes("1"));
//            Result result = table.get(get);
//            byte[] value = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("column1"));
//            System.out.println("Value: " + Bytes.toString(value));
//            table.close();
//        }
    	
    	int f = 2; int c = 3;
    	
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
    	
    		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("measure"));
    		for (int m = 1; m <= measures; m++) {
    			tableDescriptor.addFamily(new HColumnDescriptor(String.format("measure%d", m)));
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
    private static void bootstrapping(int f, int c) {
    	ArrayList<MeterReading> meterReadings = readCsvToConsole(csvFilepath, csvDelimiter);
    }
    
    public static ArrayList<MeterReading> readCsvToConsole(String csvFilePath, String csvDelimiter) {
        String line;                            // To hold each valid data line.
        ArrayList<MeterReading> meterReadings = new ArrayList<MeterReading>();

        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {

            while ((line = br.readLine()) != null) {
                String[] values = line.split(csvDelimiter);
                MeterReading mr = new MeterReading(values[0], values[1], values[2]);
                meterReadings.add(mr);
                System.out.println(mr.toString());
            }
            
            System.out.println(meterReadings.size()); 
        }
        // Trap these Exceptions
        catch (FileNotFoundException ex) {
            System.err.println(ex.getMessage());
        }
        catch (IOException ex) {
            System.err.println(ex.getMessage());
        }
        
        return meterReadings;
    }
      
}
