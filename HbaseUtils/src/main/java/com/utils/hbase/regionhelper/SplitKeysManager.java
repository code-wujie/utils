package com.utils.hbase.regionhelper;

import org.apache.hadoop.hbase.util.Bytes;

public class SplitKeysManager implements SplitKeysCalculator {
	//预设分区
	public static final int DEFAULT_PARTITION_AMOUNT = 20;
	//分区数
	private int partition = DEFAULT_PARTITION_AMOUNT;
	//分区字典表
	private final static char[] chr_dict = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
	
	/**
	 * 设置分区数
	 * @param partition	分区数
	 */
	public SplitKeysManager setPartition(int partition) {
		this.partition = partition;
		return this;
	}
	
	/**
	 * 获取预分区
	 */
	public byte[][] calcSplitKeys() {
		//生成字典映射表
		String[] dict = new String[chr_dict.length * chr_dict.length * chr_dict.length];
		int num = 0;
		for(char c1 : chr_dict){
			for(char c2 : chr_dict){
				for(char c3 : chr_dict){
					dict[num] = String.valueOf(c1) + String.valueOf(c2) + String.valueOf(c3);
					num ++ ;
				}
			}
		}
		//修正
		if(partition<=0){
			partition = 1;
		}
		//根据分区数，创建预分区
		byte[][] splitKeys = new byte[partition-1][];
		int group_length = dict.length / (partition);
		int this_partition = 0;
		for(int i=group_length; i<dict.length; i=i+group_length){
			splitKeys[this_partition] = Bytes.toBytes(dict[i]);
			this_partition ++ ;
			if(this_partition >= partition-1){
				break;
			}
		}
		//返回
		return splitKeys;
	}
}
