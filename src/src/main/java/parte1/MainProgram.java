package parte1;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;

public class MainProgram {


    
    public static void main(String[] args) throws IOException {
        try (Connection connection = HBaseConnector.getConnection()) {
            Table table = connection.getTable(TableName.valueOf("demo"));
            Get get = new Get(Bytes.toBytes("1"));
            Result result = table.get(get);
            byte[] value = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("column1"));
            System.out.println("Value: " + Bytes.toString(value));
            table.close();
        }
    }
      
}
