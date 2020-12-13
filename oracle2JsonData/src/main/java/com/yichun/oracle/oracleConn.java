package com.yichun.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class oracleConn {
    static String url = "jdbc:oracle:thin:@191.180.0.38:1521:orcl";
    static String user = "system";
    static String password = "123456";
    static String dbname = "USERGIRD";

    public static Connection getconn() throws ClassNotFoundException, SQLException {
        Connection conn = null;
        Class.forName("oracle.jdbc.driver.OracleDriver");
        //驱动成功后进行连接
        conn = DriverManager.getConnection(url, user, password);
        return conn;
    }
}
