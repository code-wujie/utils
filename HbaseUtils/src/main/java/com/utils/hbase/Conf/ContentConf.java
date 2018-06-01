package com.utils.hbase.Conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by WJ on 2018/6/1.
 */
public  class ContentConf {
    static {
        //该用户拥有执行操作hbase的权限；
        System.getProperties().setProperty("HADOOP_USER_NAME", "bbddataom");
    }


    public static Configuration GetHbaseConf(){
        Configuration conf= HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","10.28.102.151");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("zookeeper.znode.parent","/hbase");
        //conf.set("hadoop.security.authentication","kerberos");
        //conf.set("hbase.security.authentication","kerberos");
        return conf;
    }

    public static Map<String,String> getOtherConf(){
        Map<String,String> map=new HashMap<String, String>();
        map.put("family","info");
        map.put("table","test");
        return map;
    }

}
