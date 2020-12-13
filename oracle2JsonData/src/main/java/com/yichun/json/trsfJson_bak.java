package com.yichun.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.yichun.oracle.tableName;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class trsfJson_bak {
    public static JSONArray getResult(String tablename) throws SQLException, ClassNotFoundException {
        ArrayList tables = tableName.getTable();
        Iterator it = tables.iterator();
        JSONArray array = new JSONArray();
        while (it.hasNext()) {
            //每个表名
            String table = it.next().toString();
            ResultSet rs = tableName.getResult(table);
            //获取表结构
            ResultSetMetaData metaData = rs.getMetaData();
            //获取行总数
            int num = metaData.getColumnCount();

            // 如果结果集中有值
            while (rs.next()) {
                // 创建json对象就是一个{name:wp}
                Map map = new HashMap();
                for (int i = 1; i <= num; i++) {
                    // 添加键值对，比如说{name:Wp}通过name找到wp
                    map.put(metaData.getColumnName(i), rs.getObject(i));
                    JSON.toJSONString(map, SerializerFeature.DisableCircularReferenceDetect);
                }
                array.add(map);
            }

        }
        return array;
    }
}
