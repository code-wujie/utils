package com.utils.es.model;

import java.io.Serializable;
import java.util.UUID;

public class IndexModel implements Serializable{
	
	private static final long serialVersionUID = -7496656346114125044L;
	
	private String _index;
	private String _type;
	private String _id;
	
	public IndexModel(String _index, String _type) {
		this._index = _index;
		this._type = _type;
		this._id = UUID.randomUUID().toString();
	}
	
	public IndexModel(String _index, String _type, String _id) {
		this._index = _index;
		this._type = _type;
		this._id = _id;
	}

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

	@Override
	public String toString() {
		return "IndexModel [_index=" + _index + ", _type=" + _type + ", _id=" + _id + "]";
	}
	
	public String toUri(){
		return "/" + _index + "/" + _type + "/" + _id;
	}
	
	public boolean ok(){
		if(_index != null && _type != null && _id != null && !_index.trim().isEmpty() && !_type.trim().isEmpty() && !_id.trim().isEmpty()){
			return true;
		}
		else{
			return false;
		}
	}
	
}
