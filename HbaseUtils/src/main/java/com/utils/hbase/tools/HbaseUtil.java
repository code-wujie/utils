package com.utils.hbase.tools;

import com.utils.hbase.model.InsertModel;
import com.utils.hbase.model.InsertModel;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

public class HbaseUtil {


	/**
	 * 获取一个put
	 * 参数为前面创建的insetmodel
	 * @return put
	 */
	public static Put getPut(InsertModel insertModel){
		if(insertModel == null){
			return null;
		}
		//获取数据
		String row_id = insertModel.getRow_id();
		HashMap<String, Map<String, Object>> datas = insertModel.getData();
		//判断
		if(row_id == null ||row_id.trim().isEmpty() || datas == null || datas.isEmpty()){
			return null;
		}
		else{
			//生成put
			Put put = new Put(ByteUtil.toStringBytes(row_id));
			for(String family : datas.keySet()){
				Map<String, Object> values = datas.get(family);
				if(values != null && !values.isEmpty()){
					for(String key : values.keySet()){
						Object value = values.get(key);
						if(value == null){
							put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), null);
						}
						else{
							put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), ByteUtil.toStringBytes(value));
						}
					}
				}
			}
			//返回数据
			return put;
		}
	}
}
