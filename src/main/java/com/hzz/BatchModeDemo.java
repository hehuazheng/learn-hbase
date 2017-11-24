package com.hzz;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class BatchModeDemo {
    public static void main(String[] args) throws Exception {
        Connection conn = ConnectionFactory.createConnection();
        Table table = conn.getTable(TableName.valueOf("test"));
        List<Row> list = Lists.newArrayList();
        Delete delete = new Delete(Bytes.toBytes("row1"));
        list.add(delete);
        System.out.println("delete column");
        delete.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("b"));
        Put put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("a"), Bytes.toBytes("hello"));
        list.add(put);
        Object[] objs = new Object[list.size()];
        table.batch(list, objs);
        for (Object o : objs) {
            System.out.println("objs: " + o.toString());
        }
        table.close();
    }
}
