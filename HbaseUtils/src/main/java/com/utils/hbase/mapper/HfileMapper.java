package com.utils.hbase.mapper;


import com.utils.hbase.Conf.ContentConf;
import com.wujie.util.JsonUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by WJ on 2018/5/31.
 */
public class HfileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    static Logger logger = LoggerFactory.getLogger(HfileMapper.class);
    /*
    获取数据，然后对数据进行格式转换为hfile的格式
    原始数据为json的格式。
    最后context发送出去的数据是ImmutableBytesWritable，Put
     */
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
        String valuestr = value.toString();
        Map<String,Object> data=new HashMap<String, Object>();
        data= JsonUtils.JsonToMap(valuestr);
        String hkey=null;
        if(data.containsKey("rowkey")){
            hkey = data.get("rowkey").toString();
        }else if (data.containsKey("rk")){
            hkey = data.get("rk").toString();
        }else if(data.containsKey("hbase_rowkey")){
            hkey = data.get("rk").toString();
        }else{
            System.exit(1);
        }
        final byte[] rowKey = Bytes.toBytes(hkey);
        final ImmutableBytesWritable HKey = new ImmutableBytesWritable(rowKey);
        Put HPut = new Put(rowKey);

        String family=null;
        if (ContentConf.getOtherConf().containsKey("family")){
            family=ContentConf.getOtherConf().get("family");
        }else{
            family="info";
        }

        for(Map.Entry<String,Object> entity:data.entrySet()){
            String column = entity.getKey();
            String c_value=JsonUtils.BeanToJsonStr(entity.getValue());
            HPut.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(c_value));
        }
        context.write(HKey, HPut);
    }

}
