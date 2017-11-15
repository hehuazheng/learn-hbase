package com.hzz.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;

import java.io.IOException;
import java.util.Map;

/**
 * Created by hejf on 2017/11/15.
 */
public class ImportTextToHbaseReducer extends TableReducer<Text, MapWritable, ImmutableBytesWritable> {
    private Counter reducerCounter;

    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
        reducerCounter = context.getCounter(CountEnum.REDUCER);
        reducerCounter.increment(1);

        Configuration conf = context.getConfiguration();
        String familyName = conf.get("family.name");
        if (familyName == null || "".equals(familyName)) {
            return;
        }

        Put put = new Put(Bytes.toBytes(key.toString()));
        for (MapWritable v : values) {
            for (Map.Entry<Writable, Writable> entry : v.entrySet()) {
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(entry.getKey().toString()), Bytes.toBytes(entry.getValue().toString()));
            }
        }
        context.write(null, put);
    }
}