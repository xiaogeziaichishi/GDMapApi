package com.yichun.json;

//import com.alibaba.fastjson.JSONArray;

import com.yichun.oracle.tableName;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

public class getResult {
    //循环表名输出JSON文件
    public static void getData(JSONArray jsonArray, String table, int number) throws SQLException, ClassNotFoundException, IOException {
        //ArrayList tables = tableName.getTable();
        //Iterator it = tables.iterator();
        //while (it.hasNext()) {
        //每一个表
        //String table = it.next().toString();
        //每个表的每一个结果
        //JSONArray jsonArray = tabTrfJson.changeJSON(table,whereSql);
        String resJson = jsonArray.toJSONString(1);

        //目录路径
        String dirPath = "D:/data/ZZ_%s/20200917/";
        String formatDirPath = String.format(dirPath, table);
        //文件名字
        String filePath = "ZZ_%s_%s.json";
        String formatFilePath = String.format(filePath, table, number);

        File file = new File(formatDirPath);
        File file1 = new File(formatDirPath + formatFilePath);
        //创建目录
        if (!file.isDirectory()) {
            file.mkdirs();
            //System.out.println("创建目录成功！");
            //System.out.println(formatDirPath);
        } else {
            //System.out.println("目录已存在！");
            //System.out.println(formatDirPath);
        }

        //创建文件
        if (!file1.exists()) {
            file1.createNewFile();
            System.out.println("文件创建成功！");
            //System.out.println(formatDirPath + formatFilePath);
        } else {
            System.out.println("文件已经存在！");
            //System.out.println(formatDirPath + formatFilePath);
        }

        FileOutputStream outputStream = new FileOutputStream(file1, true);
        OutputStreamWriter os = new OutputStreamWriter(outputStream);
        BufferedWriter bufferedWriter = new BufferedWriter(os);

        bufferedWriter.write(resJson);
        bufferedWriter.flush();
        //System.out.println("ZZ_"+table+"写入成功！");
        bufferedWriter.close();
        os.close();
        outputStream.close();
        //System.out.println("ZZ_"+table+"关闭流!");
        //}
    }
}
