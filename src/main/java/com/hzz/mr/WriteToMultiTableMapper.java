package com.hzz.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by hejf on 2017/11/16.
 */
public class WriteToMultiTableMapper extends Mapper<Object, Text, ImmutableBytesWritable, Put> {
    private Counter mapperCounter;
    private byte[] familyNameBytes;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.familyNameBytes = context.getConfiguration().get("family.name").getBytes();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        mapperCounter = context.getCounter(CountEnum.MAPPER);
        mapperCounter.increment(1);
        String line = value.toString();
        if (line == null || "".equals(line)) {
            return;
        }
        String[] cols = line.split(";");
        Put put1 = null;
        for (String col : cols) {
            String[] arr = col.split(":");
            if ("k".equals(arr[0])) {
                put1 = new Put(arr[1].getBytes());
            } else {
                put1.addColumn(familyNameBytes, arr[0].getBytes(), arr[1].getBytes());
            }
        }
        context.write(new ImmutableBytesWritable("hzz_test".getBytes()), put1);
        context.write(new ImmutableBytesWritable("hzz_test2".getBytes()), put1);
    }
}
