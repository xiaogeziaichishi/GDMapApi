package com.yichun.json;

//import com.alibaba.fastjson.JSONArray;

import com.yichun.oracle.oracleConn;
import com.yichun.oracle.tableName;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;

public class rowSpiltJson {
    public static void spiltData(int sizenum) throws SQLException, ClassNotFoundException, IOException {
        ArrayList tables = tableName.getTable();

        Iterator it = tables.iterator();
        while (it.hasNext()) {
            //每一个表
            String table = it.next().toString();
            //总行数
            int rowNum = tableName.getRowNum(table);
            double totalnum = (double) rowNum / sizenum;
            //总块数
            int allnum = (int) Math.ceil(totalnum);
            long begin = 1;
            long end = sizenum;
            for (int i = 1; i <= allnum; i++) {
                //String whereSql = "WHERE ROWNUM >=" + begin + " and ROWNUM <= " + end;
                String whereSql = "(select ROWNUM rn, t.* from USERGIRD." + table + " t ) a where rn >= " + begin + " and rn <=" + end;
                System.out.println(whereSql);
                ResultSet result = tableName.getResult(whereSql);
                cn.hutool.json.JSONArray objects = tabTrfJson.changeJSON(result);
                getResult.getData(objects, table, i);

                double b = (double) i / allnum;
                double v = b * 100;
                String format = String.format("%.2f", v);
                System.out.println(table + "========>" + format + "%");
                begin += sizenum;
                end += sizenum;
            }


        }

    }

    //单个表，多个表执行
    public static void getAppointTbaleData(String table, int sizenum, int rowNum) throws SQLException, IOException, ClassNotFoundException {
        //int sizenum = 5000;
        //String table = "VW_ATTENTION_LINE";
        //总行数
        //int rowNum = tableName.getRowNum(table);
        //String table = "";
        //int sizenum
        //int rowNum  = 5833173;
        double totalnum = (double) rowNum / sizenum; //6.5

        //总块数
        int allnum = (int) Math.ceil(totalnum);

        long begin = 1;
        long end = sizenum;
        for (int i = 1; i <= allnum; i++) {
            String whereSql = "(select ROWNUM rn, t.* from USERGIRD." + table + " t ) a where rn >= " + begin + " and rn <=" + end;
            ResultSet result = tableName.getResult(whereSql);
            System.out.println("执行sql完毕" + whereSql);
            JSONArray objects = tabTrfJson.changeJSON(result);
            getResult.getData(objects, table, i);
            //进度条
            double b = (double) i / allnum;
            double v = b * 100;
            String format = String.format("%.2f", v);
            System.out.println(table + "========>" + format + "%");

            begin += sizenum;
            end += sizenum;
        }

    }


}
