package com.utils.hbase.Export;

import com.utils.hbase.BulkLoad.BulkloadJob;
import com.utils.hbase.BulkLoad.Conf.ContentConf;
import com.utils.hbase.Export.mapper.HbaseToHdfsMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by WJ on 2018/6/2.
 */
public class HBaseToHdfs {
    static Logger logger = LoggerFactory.getLogger(BulkloadJob.class);
    public static void ExportData(String table,String outpath){
        Configuration conf = ContentConf.GetHbaseConf();

        Job job = null;
        try {
            job = Job.getInstance(conf, "export-data-from-hbase-table"+table);
            job.setJarByClass(HBaseToHdfs.class);

            job.setMapperClass(HbaseToHdfsMapper.class);
            job.setNumReduceTasks(0);

            //此处设置key时，需要与mapper里面保持一致。
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);

            TableMapReduceUtil.initTableMapperJob(table, new Scan(),HbaseToHdfsMapper.class ,Text.class, Text.class, job);

            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(outpath));

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        if (args.length < 1) {
            logger.error("请输入参数：outpath and table if you want to settings");
        } else if(args.length==1){
            String outpath = args[0];
            String table=null;
            if(ContentConf.getOtherConf().containsKey("table")){
                table=ContentConf.getOtherConf().get("table");
            }else{
                table="test";
            }
            ExportData(table, outpath);
        } else{
            String outpath = args[0];
            String table = args[1];
            ExportData(table, outpath);
        }
    }
}
