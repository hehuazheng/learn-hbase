package com.hzz.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

/**
 * Created by hejf on 2017/11/15.
 */
public class ImportTextToMultiTable extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static void main(String[] args) {
        try {
            int ret = ToolRunner.run(new ImportTextToMultiTable(), args);
            System.exit(ret);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance();
        Configuration conf = job.getConfiguration();
        conf.set("hbase.zookeeper.quorum", args[0]);
        conf.set("mapreduce.job.maps", args[1]);
        conf.set("mapreduce.job.reduces", args[2]);
        conf.set("zookeeper.znode.parent", args[3]);
        conf.set("family.name", args[4]);
        job.setJobName(ImportTextToMultiTable.class.getSimpleName());
        job.setJarByClass(ImportTextToMultiTable.class);
        job.setOutputFormatClass(MultiTableOutputFormat.class);
        job.setMapperClass(WriteToMultiTableMapper.class);
//        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
//        job.setMapOutputValueClass(Put.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(0);
        TableMapReduceUtil.addDependencyJars(job);
        TableMapReduceUtil.addDependencyJars(job.getConfiguration());
        FileInputFormat.addInputPath(job, new Path(args[5]));
        boolean success = job.waitForCompletion(true);
        Counters counters = job.getCounters();
        LOG.info("mapper counter " + counters.findCounter(CountEnum.MAPPER));
        return success ? 0 : 1;
    }
}






