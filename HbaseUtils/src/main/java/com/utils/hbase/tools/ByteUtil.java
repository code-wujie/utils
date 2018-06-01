package com.utils.hbase.tools;

import com.utils.hbase.tools.*;
import com.wujie.util.JsonUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.web.JsonUtil;

import java.util.List;
import java.util.Map;


public class ByteUtil {
	
	/**
	 * 将数据转换为字节数字
	 * @param param
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static byte[] toBytes(Object param){
		if(param == null){
			return null;
		}
		else if (param instanceof Integer) {
		    int value = ((Integer) param).intValue();
		    return Bytes.toBytes(String.valueOf(value));
		} 
		else if (param instanceof String) {
		    String value = (String) param;
		    return Bytes.toBytes(value);
		} 
		else if (param instanceof Double) {
		    double value = ((Double) param).doubleValue();
		    return Bytes.toBytes(value);
		} 
		else if (param instanceof Float) {
		    float value = ((Float) param).floatValue();
		    return Bytes.toBytes(value);
		} 
		else if (param instanceof Long) {
		    long value = ((Long) param).longValue();
		    return Bytes.toBytes(value);
		} 
		else if (param instanceof Boolean) {
		    boolean value = ((Boolean) param).booleanValue();
		    return Bytes.toBytes(value);
		}
		else if (param instanceof Map) {
			Map<String, Object> value = (Map<String, Object>) param;
		    return Bytes.toBytes(JsonUtils.BeanToJsonStr(value));
		}
		else if (param instanceof List) {
			List<Object> value = (List<Object>) param;
		    return Bytes.toBytes(JsonUtils.BeanToJsonStr(value));
		}
		else{
			return Bytes.toBytes(String.valueOf(param));
		}
	}
	
	/**
	 * 将数据转换为字节数字,(备注：这里会将数据全部先转换为String)
	 * @param param
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static byte[] toStringBytes(Object param){
		if(param == null){
			return null;
		}
		else if (param instanceof Map) {
			Map<String, Object> value = (Map<String, Object>) param;
		    return Bytes.toBytes(JsonUtils.BeanToJsonStr(value));
		}
		else if (param instanceof List) {
			List<Object> value = (List<Object>) param;
		    return Bytes.toBytes(JsonUtils.BeanToJsonStr(value));
		}
		else{
			return Bytes.toBytes(String.valueOf(param));
		}
	}
	
}
