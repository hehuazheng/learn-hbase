package com.hzz;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * java -cp learn-hbase-1.0-SNAPSHOT.jar:hbase-server-1.1.2.jar:hbase-client-1.1.2.jar:hbase-common-1.1.2.jar:hadoop-common-2.7.1.jar:commons-logging-1.2.jar:guava-12.0.jar:commons-collections-3.2.1.jar:commons-configuration-1.6.jar:commons-lang-2.6.jar:hadoop-auth-2.5.1.jar:slf4j-api-1.7.0.jar:zookeeper-3.4.6.jar:hbase-protocol-1.1.2.jar:protobuf-java-2.5.0.jar:htrace-core-3.1.0-incubating.jar:netty-all-4.0.23.Final.jar:commons-codec-1.9.jar com.hzz.App  cf test
 */
public class App {
    public static void main(String[] args) throws IOException {
        String familyName = args[0];
        String tableName = args[1];
        org.apache.commons.codec.binary.Hex

                al;
        System.out.println("family name: " + familyName + ", tableName: " + tableName);
        Connection conn = ConnectionFactory.createConnection();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(familyName));
        scan.setCaching(500);
        scan.setMaxVersions(2);
        scan.setBatch(1000);
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            for (KeyValue kv : r.raw()) {
                System.out.println(String.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.",
                        Bytes.toString(kv.getRow()),
                        Bytes.toString(kv.getFamily()),
                        Bytes.toString(kv.getQualifier()),
                        Bytes.toString(kv.getValue()),
                        kv.getTimestamp()));
                System.out.println();
            }
        }
        rs.close();
    }
}
