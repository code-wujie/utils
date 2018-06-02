package com.utils.hbase.Export.mapper;

import com.wujie.util.JsonUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Created by WJ on 2018/6/2.
 */
public class HbaseToHdfsMapper extends TableMapper<NullWritable, Text> {
    String family = "info";
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //默认将数据导出为json的格式。
        NavigableMap<byte[], byte[]> familyMap = value.getFamilyMap(Bytes.toBytes(family));
        Map<String,String> map=new HashMap<String, String>();
        for(Map.Entry<byte[],byte[]> entity:familyMap.entrySet()){
            String column_key=new String(entity.getKey());
            String column_value=new String(entity.getValue());
            map.put(column_key,column_key);
        }
        //发送数据不需要key，将其设置为NullWritable
        context.write(NullWritable.get(),new Text(JsonUtils.BeanToJsonStr(map)));
    }
}
