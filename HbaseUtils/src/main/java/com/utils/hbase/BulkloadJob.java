package com.utils.hbase;

import com.utils.hbase.Conf.ContentConf;
import com.utils.hbase.mapper.HfileMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by WJ on 2018/5/31.
 */
public class BulkloadJob {
    static Logger logger = LoggerFactory.getLogger(BulkloadJob.class);

    public static void loadFile(String inpath, String outpath, String tablename) {
        Configuration conf = ContentConf.GetHbaseConf();
        try {
            Job job = Job.getInstance(conf, "change-data-to-hfile");
            job.setJarByClass(BulkloadJob.class);
            //设置map类及无reduce
            job.setMapperClass(HfileMapper.class);
            job.setNumReduceTasks(0);
            //设置输出的key和value的类型，这个和hfile对应着
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);
            //设置输入数据的格式，输出数据的格式
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(HFileOutputFormat2.class);
            //设置输入路径，输出路径
            FileInputFormat.setInputPaths(job, new Path(inpath));
            FileOutputFormat.setOutputPath(job, new Path(outpath));
            //对hfile的文件指定table名称。
            HTable table = new HTable(conf, tablename);
            HFileOutputFormat2.configureIncrementalLoad(job, table);

            if (job.waitForCompletion(true)) {
                logger.info("数据全部转为hfile格式，现在将数据放到hbase集群的表中；");
                FsShell shell = new FsShell(conf);
                try {
                    shell.run(new String[]{"-chmod", "-R", "777", outpath});
                } catch (Exception e) {
                    logger.error("Couldnt change the file permissions ", e);
                }
                //载入到hbase表
                LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
                loader.doBulkLoad(new Path(outpath), table);
            } else {
                logger.error("change hfile fail ,make sure you job settings.");
                System.exit(1);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            logger.error("参数不正确，输入路径 输出路径 tablename");
        } else if(args.length==3){
            String inpath = args[0];
            String outpath = args[1];
            String table=null;
            if(ContentConf.getOtherConf().containsKey("table")){
                table=ContentConf.getOtherConf().get("table");

            }else{
                table="test";
            }

            loadFile(inpath, outpath, table);
        } else{
            String inpath = args[0];
            String outpath = args[1];
            String table = args[2];
            loadFile(inpath, outpath, table);
        }

    }
}
