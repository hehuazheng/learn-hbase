package com.hzz;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteDemo {
    public static void main(String[] args) throws Exception {
        String type = args[0];
        Connection conn = ConnectionFactory.createConnection();
        Table table = conn.getTable(TableName.valueOf("test"));
        Delete delete = new Delete(Bytes.toBytes("row1"));
        if("1".equals(type)) {
            System.out.println("delete column");
            delete.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("b"));
        }else if("2".equals(type)) {
            System.out.println("delete all columns");
            delete.addColumns(Bytes.toBytes("cf"), Bytes.toBytes("b"));
        } else {
            System.out.println("delete row");
        }
        table.delete(delete);
        table.close();
    }
}
