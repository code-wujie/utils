package com.utils.es.model;

import java.io.Serializable;
import java.util.Map;

public class GetModel implements Serializable{
	
	private static final long serialVersionUID = 199347125804327472L;
	
	private String _index;
	private String _type;
	private String _id;
	private String _version;
	private boolean found;
	private Map<String, Object> _source;
	
	
	public String get_index() {
		return _index;
	}
	public void set_index(String _index) {
		this._index = _index;
	}
	public String get_type() {
		return _type;
	}
	public void set_type(String _type) {
		this._type = _type;
	}
	public String get_id() {
		return _id;
	}
	public void set_id(String _id) {
		this._id = _id;
	}
	public String get_version() {
		return _version;
	}
	public void set_version(String _version) {
		this._version = _version;
	}
	public boolean isFound() {
		return found;
	}
	public void setFound(boolean found) {
		this.found = found;
	}
	public Map<String, Object> get_source() {
		return _source;
	}
	public void set_source(Map<String, Object> _source) {
		this._source = _source;
	}
	
	@Override
	public String toString() {
		return "GetModel [_index=" + _index + ", _type=" + _type + ", _id=" + _id + ", _version=" + _version
				+ ", found=" + found + ", _source=" + _source + "]";
	}
	
}
