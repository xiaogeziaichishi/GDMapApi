package com.yichun.oracle;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;

//获取表名
public class tableName {
    public static ArrayList getTable() throws SQLException, ClassNotFoundException {
        ArrayList<String> list = new ArrayList<String>();
        Connection conn = oracleConn.getconn();
        Statement statement = conn.createStatement();
        String sql = "select table_name from dba_tables where owner='USERGIRD'";
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {  //循环遍历结果集
            String table_name = rs.getString("table_name");
            list.add(table_name);
            // String id=rs.getString("ID");
            //String name=rs.getString("NAME");
            //int score=rs.getInt("PROTECT_EMP_COUNT");
        }
        conn.close();
        return list;

    }

    public static ArrayList getTableColumnName(String tablename) throws SQLException, ClassNotFoundException {
        Connection conn = oracleConn.getconn();
        Statement st = null;
        ArrayList<String> colName = new ArrayList<>();

        try {
            st = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            String sql = "select * from  USERGIRD.%s";
            String formatsql = String.format(sql, tablename);
            ResultSet rs = st.executeQuery(formatsql);
            ResultSetMetaData rsm = rs.getMetaData();
            for (int i = 1; i <= rsm.getColumnCount(); i++) {
                colName.add(rsm.getColumnLabel(i));
            }

            rs.close();
            st.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return colName;
    }


    public static ResultSet getResult(/*String table,*/ String whereSql) throws SQLException, ClassNotFoundException {
        Connection conn = oracleConn.getconn();
        //ArrayList tables = tableName.getTable();
        //Iterator it = tables.iterator();

        //String sql = "select * from  USERGIRD.%s ";
        String sql = "select * from  ";
        //String formatsql = String.format(sql, table);
        String formatsql = String.format(sql);
        String resSql = formatsql + whereSql;
        Statement st = conn.createStatement();
        ResultSet resultSet = st.executeQuery(resSql);
        //System.out.println(resSql);
        //conn.close();
        return resultSet;
    }

    public static int getRowNum(String tablename) throws SQLException, ClassNotFoundException {
        Connection conn = oracleConn.getconn();
        String sql = "SELECT * FROM   USERGIRD.%s ";
        String formatsql = String.format(sql, tablename);
        Statement st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet resultSet = st.executeQuery(formatsql);
        int rowCount = 0;
        while (resultSet.next()) {
            rowCount++;
        }
        //resultSet.last();
        //int row = resultSet.getRow();
        conn.close();
        return rowCount;
    }

}
