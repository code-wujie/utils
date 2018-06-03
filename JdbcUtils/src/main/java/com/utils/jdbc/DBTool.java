package com.utils.jdbc;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Created by WJ on 2018/6/3.
 * 备注：增删改等操作，不适合hive.hive一般只做查询操作。
 */
public class DBTool {

    /**
     * 获取链接
     *
     * @param properties 参数必须包含以下值
     *                   DBname 所链接的数据库类型，只能为mysql oracle hive中一个
     *                   url    所链接的库的URL
     *                   user   用户
     *                   password   密码  hive中不用密码时，该值给为""
     *                   其余两个为可选的   minPoolSize     maxPoolSize
     * @return
     */
    public static Connection getconn(Properties properties) {
        try {
            return Pool.getInstance(properties).getConnection();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (PropertyVetoException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 批量插入数据到表中
     *
     * @param conn  链接
     * @param table 插入的表名
     * @param datas 数据为list，每一个对象为一个要插入的行中的数据
     *              且为key value形式。key是表中的字段，value是插入的值。
     */
    public static void bulkLoadData(Connection conn, String table, List<Map<String, Object>> datas) {
        if (conn == null) {
            return;
        }
        try {
            Statement statement = conn.createStatement();
            for (Map<String, Object> data : datas) {
                StringBuffer sbf_key = new StringBuffer();
                StringBuffer sbf_val = new StringBuffer();
                for (Map.Entry<String, Object> entity : data.entrySet()) {
                    sbf_key.append("`").append(entity.getKey()).append("`,");
                    sbf_val.append("'").append(entity.getValue().toString()).append("',");
                }
                String sql = "insert into " + table + " (" + sbf_key.substring(0, sbf_key.length() - 1) +
                        " ) values (" + sbf_val.substring(0, sbf_val.length() - 1) + " )";
                statement.addBatch(sql);
            }
            statement.executeBatch();
            statement.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    /**
     * 执行一条sql语句，如建标，删除数据，单条数据插入或者更新等
     *
     * @param conn
     * @param table
     * @param sql   需要执行的sql语句，要确保sql的正确性。
     */
    public static void do_sql(Connection conn, String table, String sql) {
        if (conn == null) {
            return;
        }
        try {
            Statement statement = conn.createStatement();
            statement.execute(sql);
            statement.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    /**
     * 批量replace数据，可以是插入，也可以是更新。更新必须在表中创建了唯一索引。
     *
     * @param conn
     * @param table
     * @param datas
     */
    public static void bulkReplaceData(Connection conn, String table, List<Map<String, Object>> datas) {
        if (conn == null) {
            return;
        }
        try {
            Statement statement = conn.createStatement();
            for (Map<String, Object> data : datas) {
                StringBuffer sbf_key = new StringBuffer();
                StringBuffer sbf_val = new StringBuffer();
                for (Map.Entry<String, Object> entity : data.entrySet()) {
                    sbf_key.append("`").append(entity.getKey()).append("`,");
                    sbf_val.append("'").append(entity.getValue().toString()).append("',");
                }
                String sql = "replace into " + table + " (" + sbf_key.substring(0, sbf_key.length() - 1) +
                        " ) values (" + sbf_val.substring(0, sbf_val.length() - 1) + " )";
                statement.addBatch(sql);
            }
            statement.executeBatch();
            statement.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    /**
     * 从数据库中查询数据
     *
     * @param connection
     * @param sql        查询的sql
     * @param felids     返回的字段
     * @param limit      返回的条数
     * @param dbname     查询的库。
     * @return
     */
    public static List<Map<String, Object>> select_sql(Connection connection, String sql, List<String> felids, int limit, String dbname) {
        if (connection == null) {
            return null;
        }
        //对sql的limit进行限制。
        if ("oracle".equals(dbname)) {
            //oracle中不支持limit的写法
            if (!sql.contains("rownum")) {
                //没有做返回条数限制，则需要加上
                if (sql.contains("where")) {
                    sql = sql + " and rownum <=" + limit;
                } else {
                    sql = sql + " where rownum <=" + limit;
                }
            }
        } else {
            //hive 和mysql支持limit写法
            if (!sql.contains("limit")) {
                sql = sql + " limit " + limit;
            }
        }
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            List<Map<String, Object>> datas = new ArrayList<Map<String, Object>>();
            while (resultSet.next()) {
                Map<String, Object> data = new HashMap<String, Object>();
                for (String key : felids) {
                    //不能够获取到获得数据的类型，全部统一转为string。
                    String value = resultSet.getString(key);
                    data.put(key, value);
                }
                datas.add(data);
            }
            resultSet.close();
            statement.close();
            connection.close();
            return datas;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        Properties properties=new Properties();
        properties.put("DBname","mysql");
        properties.put("url","jdbc:mysql://localhost:3306/test?characterEncoding=UTF-8");
        properties.put("user","root");
        properties.put("password","root");

        Connection getconn = getconn(properties);
        List<String> felids=new ArrayList<String>();
        felids.add("id");
        felids.add("ip");
        List<Map<String, Object>> list = select_sql(getconn, "select * from task3", felids, 5, "mysql");
        System.out.println(list);
    }
}
