package com.utils.es.model;

import java.util.HashMap;
import java.util.Map;

public class SourceModel extends HashMap<String, Object>{

	private static final long serialVersionUID = -3351627387816570602L;
	
	public SourceModel(){}
	
	public SourceModel(Map<String, Object> map){
		this.putAll(map);
	}
	
	//添加key-value
	public SourceModel add(String key, Object value){
		this.put(key, value);
		return this;
	}
	
	//添加map
	public SourceModel add(Map<String, Object> map){
		this.putAll(map);
		return this;
	}
	
}
