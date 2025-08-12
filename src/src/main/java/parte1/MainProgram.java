package parte1;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Connection;

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
    	
    	bootstrapping(2, 3);
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
