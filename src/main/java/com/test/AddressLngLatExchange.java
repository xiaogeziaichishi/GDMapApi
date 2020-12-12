package com.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liuguibin
 * @date 2020-12-12 13:19
 */
public class AddressLngLatExchange {
    private static final Logger logger = LoggerFactory.getLogger(AddressLngLatExchange.class);
    private AddressLngLatExchange(){
        try {
            throw new IllegalAccessException( "Utility class");
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

     public static String getLngLat(String addr) {
        StringBuilder json = new StringBuilder();
        HttpURLConnection conn = null;
        BufferedReader br = null;
        //https://restapi.amap.com/v3/geocode/geo?address=北京市朝阳区阜通东大街6号&output=XML&key=<用户的key>
        String httpUrl = Parameters.URL + Parameters.GDKEY + Parameters.FLAG + "address=" + addr + Parameters.FLAG + Parameters.OUTFRAMT;
        try {
            URL url = new URL(httpUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.connect();//建立连接
            if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
                String inputline = null;
                while ((inputline = br.readLine()) != null) {
                    json.append(inputline);
                }
                logger.info("返回值{}", json);
            }
        } catch (MalformedURLException e) {
            //url地址异常
            logger.info("{}异常，查看！", httpUrl);
            e.printStackTrace();
        } catch (IOException e) {
            //连接异常
            logger.info("连接异常！");
            e.printStackTrace();
        }
        return json.toString();
    }
}
