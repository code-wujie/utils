package com.utils.hbase.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * hbase的存储模型，
 * rowkey 与 一个map类型构成；map即为一个列簇里面的所有列和对应的value。
 */
public class InsertModel implements Serializable{

	private static final long serialVersionUID = 2790414231643561915L;
	
	private String row_id;
	private HashMap<String, Map<String, Object>> data = new HashMap<String, Map<String, Object>>();
	
	public InsertModel(String row_id) {
		this.row_id = row_id;
	}
	
	public InsertModel(String row_id, HashMap<String, Map<String, Object>> data) {
		this.row_id = row_id;
		this.data = data;
	}

	public String getRow_id() {
		return row_id;
	}

	public void setRow_id(String row_id) {
		this.row_id = row_id;
	}

	public HashMap<String, Map<String, Object>> getData() {
		return data;
	}

	public void setData(HashMap<String, Map<String, Object>> data) {
		this.data = data;
	}
	
	/**
	 * 添加数据（链式编程）
	 * @param family	列族
	 * @param value		值
	 * @return	当前对象
	 */
	public InsertModel addData(String family, Map<String, Object> value){
		this.getData().put(family, value);
		return this;
	}
	
	@Override
	public String toString() {
		return "InsertModel [row_id=" + row_id + ", data=" + data + "]";
	}
	
}
