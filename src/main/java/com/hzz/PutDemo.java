package com.hzz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class PutDemo {
    /**
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "c1,c2,c3");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        conf.set("zookeeper.session.timeout", "90000");
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("hzz_test"));
        Put put1 = new Put(Bytes.toBytes("row3"));
        put1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("a"), Bytes.toBytes("a"));
        put1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("b"), System.currentTimeMillis(), Bytes.toBytes("bb"));
        table.put(put1);
        table.close();
        conn.close();
    }
}
