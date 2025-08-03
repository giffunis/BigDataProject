package parte1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseConnector {
    public static Connection getConnection() throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost"); // or your ZooKeeper host
        config.set("hbase.zookeeper.property.clientPort", "2181");

        return ConnectionFactory.createConnection(config);
    }
}
