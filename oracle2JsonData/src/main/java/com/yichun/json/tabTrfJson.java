package com.yichun.json;

//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.serializer.SerializerFeature;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;
import com.yichun.oracle.tableName;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

//传入表名，输出json格式
public class tabTrfJson {
    public static JSONArray changeJSON(ResultSet rs) throws SQLException, ClassNotFoundException {
        //ResultSet rs = tableName.getResult(tblname,whereSql);
        ResultSetMetaData metaData = rs.getMetaData();
        int num = metaData.getColumnCount();  //6
        rs.getRow();
        JSONArray array = new JSONArray();

        while (rs.next()) {
            //列名
            Map map = new LinkedHashMap();
            for (int i = 1; i <= num; i++) {
                // 添加键值对，比如说{name:Wp}通过name找到wp
                Object object = rs.getObject(i);
                String s = String.valueOf(object);
                if (s == "null") {
                    s = "";
                }
                map.put(metaData.getColumnName(i), s);


                //JSON.toJSONString(map, SerializerFeature.DisableCircularReferenceDetect);
            }

            array.add(map);

        }
        return array;
    }
}
