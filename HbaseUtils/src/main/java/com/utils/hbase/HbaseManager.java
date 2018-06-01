package com.utils.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import com.utils.hbase.model.InsertModel;
import com.utils.hbase.regionhelper.SplitKeysManager;
import com.utils.hbase.tools.HbaseUtil;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;


public class HbaseManager {
    //日志对象
    public static final Logger LOGGER = Logger.getLogger(HbaseManager.class);
    //链接
    private Connection connection;
    //单例
    private volatile static ConcurrentHashMap<String, HbaseManager> utils = new ConcurrentHashMap<String, HbaseManager>();;

    //初始化相关信息，主要初始化hbase Connection 的实例
    public HbaseManager(String hosts,  Integer port,  Integer threads) throws IOException {
        //参数判断
        port = port ==null ? 2181 : port;
        threads = threads==null ? 0 : threads;
        Configuration conf = HBaseConfiguration.create();
        //生成Connection实例
        conf.set("hbase.zookeeper.quorum", hosts);
        conf.set("hbase.zookeeper.property.clientPort", String.valueOf(port));
        if(threads > 0){
            this.connection = ConnectionFactory.createConnection(conf, Executors.newFixedThreadPool(threads));
        }
        else{
            this.connection = ConnectionFactory.createConnection(conf);
        }
    }

    /**
     * 单例
     * @return	单例
     * @throws Exception  异常
     */
    public static HbaseManager getInstance() throws IOException{
        return getInstance("localhost", 2181, 0);
    }

    /**
     * 单例
     * @param hosts	主机地址
     * @param port	端口
     * @return	实例
     * @throws IOException 异常
     */
    public static HbaseManager getInstance(String hosts,  int port) throws IOException{
        return getInstance(hosts, port, 0);
    }

    /**
     * 单例
     * @param hosts	主机地址
     * @param port	端口
     * @param threads 并发线程数
     * @return	实例
     * @throws IOException 异常
     */
    public static HbaseManager getInstance(String hosts,  int port,  int threads) throws IOException {
        String hbase_key = hosts+port+threads;
        if(!utils.containsKey(hbase_key)){
            synchronized (HbaseManager.class) {
                if(!utils.containsKey(hbase_key)){
                    utils.put(hbase_key, new HbaseManager(hosts, port, threads));
                }
            }
        }
        return utils.get(hbase_key);
    }

    /**
     * 析构
     */
    @Override
    protected void finalize() throws Throwable {
        if(this.connection != null){
            this.connection.close();
        }
    }



    /**
     * 判断是否有Hbase表
     * @param tableName	表名
     * @throws IOException	异常
     */
    public boolean tableExists(String tableName) throws IOException {
        return this.connection.getAdmin().tableExists(TableName.valueOf(tableName));
    }

    /**
     * 创建Hbase表，默认预分区数量=20
     * @param tableName	表名
     * @param
     * @throws IOException	异常
     */
    public synchronized boolean createTable(String tableName, String family) throws IOException {
        Set<String> familys = new HashSet<String>();
        familys.add(family);
        return this.createTable(tableName, familys, SplitKeysManager.DEFAULT_PARTITION_AMOUNT);
    }

    /**
     * 创建Hbase表，默认预分区数量=20
     * @param tableName	表名
     * @param familys	列族列表
     * @throws IOException	异常
     */
    public synchronized boolean createTable(String tableName, Set<String> familys) throws IOException {
        return this.createTable(tableName, familys, SplitKeysManager.DEFAULT_PARTITION_AMOUNT);
    }

    /**
     * 创建Hbase表
     * @param tableName	表名
     * @param familys	列族列表
     * @param partition 预分区数量
     * @throws IOException	异常
     */
    public synchronized boolean createTable(String tableName, Set<String> familys, int partition) throws IOException {
        //判断表是否存在
        if (!this.tableExists(tableName)){
            //创建表对象
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for(String family : familys){
                //添加列族,并且设置压缩方式
                hTableDescriptor.addFamily(new HColumnDescriptor(family).setCompressionType(Compression.Algorithm.SNAPPY));
            }
            //获取分区
            byte[][] splits = new SplitKeysManager().setPartition(partition).calcSplitKeys();
            //创建表
            this.connection.getAdmin().createTable(hTableDescriptor, splits);
            return true;
        }
        else{
            return false;
        }
    }

    /**
     * 添加列族
     * @param tableName	表名
     * @param familys	列族列表
     * @throws IOException	异常
     */
    public synchronized boolean addFamily(String tableName, Set<String> familys) throws IOException {
        //判断表是否存在
        if (!this.tableExists(tableName)){
            this.createTable(tableName, familys);
            return true;
        }
        else{
            Set<byte[]> temp_familys = this.connection.getAdmin().getTableDescriptor(TableName.valueOf(tableName)).getFamiliesKeys();
            Set<String> this_familys = new HashSet<String>();
            for(byte[] old_family : temp_familys){
                this_familys.add(Bytes.toString(old_family));
            }
            familys.removeAll(this_familys);
            for(String family : familys){
                //添加列族,并且设置压缩方式
                this.connection.getAdmin().addColumn(TableName.valueOf(tableName), new HColumnDescriptor(family).setCompressionType(Compression.Algorithm.SNAPPY));
            }
            return true;
        }
    }

    /**
     * 插入数据
     * @param tableName	表名
     * @param insertModel	数据
     * @return	是否插入成功
     * @throws IOException
     */
    public boolean insert(String tableName, InsertModel insertModel) throws IOException{
        Put put = HbaseUtil.getPut(insertModel);
        if(put != null){
            Table table = this.connection.getTable(TableName.valueOf(tableName));
            try {
                table.put(put);
            } catch (Exception e) {
                //这里的异常在spark的生产环境中无法捕获，以下为折中处理
                if(e.getMessage().contains(tableName)){
                    //体检列族
                    Set<String> familys = insertModel.getData().keySet();
                    this.addFamily(tableName, familys);
                    //重新添加数据
                    table.put(put);
                }
                else{
                    LOGGER.error("数据插入失败，请对数据和表进行检查");
                }
            }
            table.close();
            return true;
        }
        return false;
    }

    /**
     * 插入数据
     * @param tableName	表名
     * @param insertModels	数据
     * @return	是否插入成功
     * @throws IOException
     */
    public boolean insert(String tableName, List<InsertModel> insertModels) throws IOException{
        if(insertModels == null || insertModels.isEmpty()){
            return false;
        }
        //获取列族
        Set<String> familys = new HashSet<String>();
        //组装puts
        List<Put> puts = new ArrayList<Put>();
        for(InsertModel insertModel : insertModels){
            Put put = HbaseUtil.getPut(insertModel);
            if(put != null ){
                puts.add(put);
            }
            familys.addAll(insertModel.getData().keySet());
        }
        //插入数据
        if(!puts.isEmpty()){
            Table table = this.connection.getTable(TableName.valueOf(tableName));
            try {
                table.put(puts);
            } catch (Exception e) {
                //这里的异常在spark的生产环境中无法捕获，以下为折中处理
                if(e.getMessage().contains(tableName)){
                    //添加列族
                    this.addFamily(tableName, familys);
                    //重新添加数据
                    table.put(puts);
                }
                else{
                    LOGGER.error("数据插入失败，请对数据和表进行检查");
                }
            }
            table.close();
            return true;
        }
        return false;
    }

    /**
     * 根据RowKey获取value
     * @param tableName	表名
     * @param get	行键
     * @throws IOException
     */
    public Cell[] getByGetToCells(String tableName, Get get) throws IOException{
        Result result = getByGetToResult(tableName, get);
        if(result == null || result.isEmpty()){
            return null;
        }
        else{
            return result.rawCells();
        }
    }

    /**
     * 根据RowKey获取value
     * @param tableName	表名
     * @param get	行键
     * @throws IOException
     */
    public Result getByGetToResult(String tableName, Get get) throws IOException{
        Table table= this.connection.getTable(TableName.valueOf(tableName));
        Result result = table.get(get);
        if(result == null || result.isEmpty()){
            return null;
        }
        else{
            return result;
        }
    }

    /**
     * 判断get是否存在，也可理解为rowkey是否存在。
     * @param tableName
     * @param rowKey
     * @return
     * @throws IOException
     */
    public boolean exists(String tableName, String rowKey) throws IOException{
        Get get = new Get(Bytes.toBytes(rowKey));
        Table table= this.connection.getTable(TableName.valueOf(tableName));
        return table.exists(get);
    }

    /**
     * 判断get是否存在
     * @param tableName
     * @param get
     * @return
     * @throws IOException
     */
    public boolean exists(String tableName, Get get) throws IOException{
        Table table= this.connection.getTable(TableName.valueOf(tableName));
        return table.exists(get);
    }

    /**
     * 根据rowkey对数据进行删除；
     * @param tableName 需要删除的rowkey
     * @param rowkey 需要删除的表明
     * @return 如果rowkey不存在，则也返回TRUE，默认删除成功。
     * @throws IOException
     */
    public boolean deleteByRowkey(String tableName,String rowkey) throws IOException {
        if(exists(tableName,rowkey)){
            Table table= this.connection.getTable(TableName.valueOf(tableName));
            Delete delete=new Delete(Bytes.toBytes(rowkey));
            try {
                table.delete(delete);
            }catch (Exception e){
                LOGGER.error("delete data fail ,and table is "+tableName+" this rowkey is "+rowkey);
                return false;
            }
            return true;
        }else {
            return true;
        }

    }

    /**
     * 根据rowkey对数据进行批量删除；
     * @param tableName
     * @param rowkeys
     * @return
     * @throws IOException
     */
    public boolean deleteByRowkey(String tableName,List<String> rowkeys) throws IOException {
        Table table= this.connection.getTable(TableName.valueOf(tableName));
        List<Delete> deletes=new ArrayList<Delete>();
        for(String rowkey:rowkeys){
            //过滤掉不存在的rowkey；
            if(exists(tableName,rowkey)){
                Delete delete=new Delete(Bytes.toBytes(rowkey));
                deletes.add(delete);
            }
        }
        if(deletes.size()>0){
            table.delete(deletes);
        }
        return true;
    }

}
