package com.utils.hbase.regionhelper;


public interface RowKeyGenerator {
	/**
	 * 获取下一个ID
	 * @return	
	 */
    byte [] nextId();
}
