package com.yichun;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.yichun.json.getResult;
import com.yichun.json.rowSpiltJson;
import com.yichun.json.tabTrfJson;
import com.yichun.oracle.tableName;


import java.io.*;

import java.sql.SQLException;
import java.util.*;

public class testData {
    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {


        //JSONArray res = tabTrfJson.changeJSON("VW_ATTENTION_LINE");

//        ResultSet rs = tableName.getResult("VW_ATTENTION_LINE");
//        ResultSetMetaData metaData = rs.getMetaData();
//        int num = metaData.getColumnCount();
//        //列数
//        System.out.println(num);
//        for (int i = 1; i <= num; i++) {
//            String columnName = metaData.getColumnName(i);
//            System.out.println(i+"-->"+columnName);
//
//        }

        //getResult.getData();
        //底下是 按照文件大小分割
//        double size = 3000;
//        String SUFFIX = ".json";
//        String a = "C:\\Users\\Raichard\\Desktop\\test\\a";
//        String formata =  a+SUFFIX;
//        System.out.println(formata);
//        File file = new File(formata);
//        String name = file.getName();
//
//        long l = file.length();
//        double d = (double) l / size;
//        //块数
//        int num = (int) Math.ceil(d);
//        //文件数据量 75个
//        String[] fileNums = new String[num];
//        FileInputStream in = new FileInputStream(file);
//        long end = 0;
//        int begin = 0;
//        for (int i = 0; i < num; i++) {
//            File outFile = new File(a + i + SUFFIX);
//            FileOutputStream out = new FileOutputStream(outFile);
//            end += size;
//            end = (end > l) ? l : end;
//            for (; begin < end; begin++) {
//                out.write(in.read());
//            }
//            out.close();
//            fileNums[i] = outFile.getAbsolutePath();
//            System.out.println(fileNums[i]);
//        }
//        in.close();
//上面是文件大小分割

        //rowSpiltJson.spiltData(5000);
        //VW_HOUSEHOLDSTAFFS,VW_HOUSEINFO,VW_BASEINFO
        Properties prop = new Properties();
        InputStream in = testData.class.getClassLoader().getResourceAsStream("application.properties");
        prop.load(in);
        //String path1 = Thread.currentThread().getContextClassLoader().getResource("application.properties").getPath();
        //String path1 = "src/main/resources/application.properties";
        //prop.load(new FileInputStream(path1));
        String table = prop.getProperty("table");
        String sizenum1 = prop.getProperty("sizenum");
        String rowNum1 = prop.getProperty("rowNum");
        int sizenum = Integer.parseInt(sizenum1);
        int rowNum = Integer.parseInt(rowNum1);
        //String table = "";
        //int sizenum = 5000;
        //int rowNum  = 5833173;
        rowSpiltJson.getAppointTbaleData(table, sizenum, rowNum);
        //System.out.println(rowNum1);
    }
}




