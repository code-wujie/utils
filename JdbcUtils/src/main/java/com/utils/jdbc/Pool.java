package com.utils.jdbc;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by WJ on 2018/6/3.
 */
public class Pool {
    private ComboPooledDataSource dataSource;
    private static Pool instance = null;

    private Pool(Properties properties) throws PropertyVetoException {
        String driver=null;
        //四个必须给出的参数
        if("mysql".equals(properties.getProperty("DBname"))){
            driver="com.mysql.jdbc.Driver";
        }else if("oracle".equals(properties.getProperty("DBname"))){
            driver="oracle.jdbc.driver.OracleDriver";
        }else if("hive".equals(properties.getProperty("DBname"))){
            driver="org.apache.hive.jdbc.HiveDriver";
        }
        dataSource=new ComboPooledDataSource();
        dataSource.setDriverClass(driver);
        dataSource.setJdbcUrl(properties.getProperty("url"));
        dataSource.setUser(properties.getProperty("user"));
        dataSource.setPassword(properties.getProperty("password"));
        //初始化大小默认设置为最小连接数。
        if(properties.containsKey("minPoolSize")){
            dataSource.setInitialPoolSize(Integer.valueOf(properties.getProperty("minPoolSize")));
            dataSource.setMinPoolSize(Integer.valueOf(properties.getProperty("minPoolSize")));
        }else{
            dataSource.setInitialPoolSize(3);
            dataSource.setMinPoolSize(3);
        }
        if(properties.containsKey("minPoolSize")){
            dataSource.setMaxPoolSize(Integer.valueOf(properties.getProperty("maxPoolSize")));
        }else{
            dataSource.setMaxPoolSize(50);
        }
        //若链接30s未使用，则丢弃该链接
        dataSource.setMaxIdleTime(30);
        //连接池中链接没有时，一次获取链接数量
        dataSource.setAcquireIncrement(3);
        //60s检查一次连接池情况
        dataSource.setIdleConnectionTestPeriod(60);
        //链接失败后，尝试3次链接
        dataSource.setAcquireRetryAttempts(3);
        //其余参数采用默认。
    }


    public synchronized static Pool getInstance(Properties properties)
            throws IOException, PropertyVetoException {
        if (instance == null) {
            instance = new Pool(properties);
        }
        return instance;
    }

    public Connection getConnection() throws SQLException {
        Connection conn = null;
        if (dataSource != null) {
            conn = dataSource.getConnection();
        }
        return conn;
    }
}
