package com.juanpi.hive;

import java.sql.*;
import java.util.logging.Logger;

/**
 * Created by wmky_kk on 21/03/2017.
 */
public class HiveJdbcClient {
    public static final Logger loger = Logger.getLogger("HiveJdbcClient.class");
    public static final String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String args[]) throws SQLException{
        try{
            Class.forName(driverName);
        }catch (ClassNotFoundException e){
            loger.info("may be HiveServer2 of the Hostname not start,try to check it with" + "\t" + "ps aux | grep -i HiveServer2");
            loger.info("Connect to Hive situation Exception" + e.getStackTrace());
//            System.exit(1);
        }
//        Connection con = DriverManager.getConnection("jdbc:hive2://192.168.19.17:10000","kaikai","kaikai");
        Connection con = DriverManager.getConnection("jdbc:hive2://192.168.19.17:10000","admin","admin");
        Statement stmt = con.createStatement();
        String tableName = "dw.dim_page";

        /*stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table " + tableName + " (key int, value string)");
        // show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }*/
        // describe table
        String sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
            loger.info(res.getString(1) + "\t" + res.getString(2));
        }

        // load data into table
        // NOTE: filepath has to be local to the hive server
        // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
        /*String filepath = "/tmp/a.txt";
        sql = "load data local inpath '" + filepath + "' into table " + tableName;
        System.out.println("Running: " + sql);
        stmt.execute(sql);*/

        // select * query
        sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
        }

        // regular hive query
        sql = "select count(*) from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
//        Exception in thread "main" java.sql.SQLException: Error while processing statement: FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.mr.MapRedTask
        //Job Submission failed with exception 'org.apache.hadoop.security.AccessControlException(Permission denied: user=kaikai, access=WRITE, inode="/user":hadoop:hadoop:drwxrwxr-x
        // 若为admin的权限就可以write hdfs上文件，则可以成功执行。
        // 额外说明下，当直接在IDEA中执行时，其实使用xshell登陆beeline的时候，可以看到详细的同步执行日志。当然也可以直接在hive的存放log处查看具体的内容
        while (res.next()) {
            System.out.println(res.getString(1));
        }
    }
}
