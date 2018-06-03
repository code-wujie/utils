package com.utils.es.utils;

import java.util.ArrayList;
import java.util.List;

public class ShardingList<E> extends ArrayList<E>{
	
	private static final long serialVersionUID = 5168554777606419731L;
	
	/**
	 * 获取分组数量
	 * @param shardingLength
	 * @return
	 */
	private int getGroup(int shardingLength){
		if(this.size() == 0){
			return 0;
		}
		else if(shardingLength <= 0){
			return 1;
		}
		else{
			Double temp = Math.ceil(this.size() * 1.0 /shardingLength);
			return temp.intValue();
		}
	}
	
	
	/**
	 * 
	 * @param shardingLength 每个分片中的数量
	 */
	public List<List<E>> sharding(int shardingLength){
		List<List<E>> lists = new ArrayList<List<E>>();
		int group = this.getGroup(shardingLength);
		for(int i=0; i < group; i++){
			if(i == (group-1) ){
				lists.add(this.subList(i * shardingLength, this.size()));
			}
			else{
				lists.add(this.subList(i * shardingLength, (i+1) * shardingLength));
			}
		}
		return lists;
	}
	
}
