package com.hzz;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by hejf on 2017/2/7.
 */
public class PutDemo {
    public static void main(String[] args) throws IOException {
        String version = args[0];
        String value = args[1];
        long longVersion = Long.parseLong(version);
        Connection conn = ConnectionFactory.createConnection();
        Table table = conn.getTable(TableName.valueOf("test"));
        Put put1 = new Put(Bytes.toBytes("row1"));
        put1.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("a"), Bytes.toBytes(value));
        put1.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("b"), longVersion, Bytes.toBytes(value));
        table.put(put1);
        table.close();
    }
}
