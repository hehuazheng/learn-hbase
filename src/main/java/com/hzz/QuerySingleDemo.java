package com.hzz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by hejf on 2017/11/24.
 */
public class QuerySingleDemo {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "c1,c2,c3");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        conf.set("zookeeper.session.timeout", "90000");
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("hzz_test"));
        Get get = new Get(Bytes.toBytes("row3"));
        get.addFamily(Bytes.toBytes("data"));
        get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("indexVersion"));
        get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("indexVersion2"));

//        put1.addColumn(, Bytes.toBytes("a"), 1500820044355L, Bytes.toBytes("a-1"));
//        put1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("b"), System.currentTimeMillis(), Bytes.toBytes("bb+1"));
//        put1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("indexVersion"), System.currentTimeMillis(), Bytes.toBytes(System.currentTimeMillis()));
//        put1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("indexVersion2"), System.currentTimeMillis(), Bytes.toBytes(String.valueOf(System.currentTimeMillis())));
        Result r = table.get(get);
        CellScanner scanner = r.cellScanner();
        while(scanner.advance()) {
            Cell cell = scanner.current();
            String colName = new String(CellUtil.cloneQualifier(cell));
            System.out.println(colName + "," + new String(CellUtil.cloneValue(cell)));
        }
        table.close();
        conn.close();
    }
}
