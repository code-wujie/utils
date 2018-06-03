package com.utils.es.model;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by WJ on 2018/6/2.
 */
public class DelModel implements Serializable {

    private static final long serialVersionUID = -1285658386162783226L;
    private String _index;
    private String _type;
    private String _id;

    public static long getSerialVersionUID() {
        return serialVersionUID;
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

    public String get_version() {
        return _version;
    }

    public void set_version(String _version) {
        this._version = _version;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public String get_seq_no() {
        return _seq_no;
    }

    public void set_seq_no(String _seq_no) {
        this._seq_no = _seq_no;
    }

    public String get_primary_term() {
        return _primary_term;
    }

    public void set_primary_term(String _primary_term) {
        this._primary_term = _primary_term;
    }

    public Map<String, Object> get_shards() {
        return _shards;
    }

    public void set_shards(Map<String, Object> _shards) {
        this._shards = _shards;
    }

    private String _version;
    private String result;
    private String _seq_no;
    private String _primary_term;
    private Map<String, Object> _shards;

}
