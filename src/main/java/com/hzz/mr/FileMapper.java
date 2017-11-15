package com.hzz.mr;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by hejf on 2017/11/15.
 */
public class FileMapper extends Mapper<Object, Text, Text, MapWritable> {
    private Counter mapperCounter;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        mapperCounter = context.getCounter(CountEnum.MAPPER);
        mapperCounter.increment(1);
        String line = value.toString();
        if (line == null || "".equals(line)) {
            return;
        }
        String[] cols = line.split(";");
        MapWritable map = new MapWritable();
        String k = null;
        for (String col : cols) {
            String[] arr = col.split(":");
            if ("k".equals(arr[0])) {
                k = arr[1];
            } else {
                map.put(new Text(arr[0]), new Text(arr[1]));
            }
        }
        context.write(new Text(k), map);
    }
}
