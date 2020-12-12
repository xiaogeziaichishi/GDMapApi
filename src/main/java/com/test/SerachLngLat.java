package com.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.Map;

/**
 * @author liuguibin
 * @date 2020-12-12 14:01
 */
public class SerachLngLat {
    public static Map<Double, Double> getValues(String addr) {
        HashMap<Double, Double> map = new HashMap<>(2);
        String strJson = AddressLngLatExchange.getLngLat(addr);
        JSONObject jsonObject = JSON.parseObject(strJson);
        String geocodes = jsonObject.get("geocodes").toString();
        JSONArray jsonArray = JSON.parseArray(geocodes);
        JSONObject jsonObjectGeocodes = JSON.parseObject(jsonArray.get(0).toString());
        String location = jsonObjectGeocodes.get("location").toString();
        String[] locationArray = location.split(",");
        map.put(Double.valueOf(locationArray[0]), Double.valueOf(locationArray[1]));
        return map;
    }
}
