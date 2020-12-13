package com.yichun.createESIndex;

import com.yichun.oracle.oracleConn;
import com.yichun.oracle.oracleTbaleColumn;
import com.yichun.oracle.tableName;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

public class createIndex {

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        String filed1 = "curl -XPUT 'http://10.178.77.8:9200/zz_";
        String filed2 = "?include_type_name=true' -H 'Content-Type:application/json' " +
                "-d '{ \"settings\" : { \"index\" : " +
                "{  \"number_of_shards\" : \"3\", \"number_of_replicas\" : \"1\" } }, " +
                "\"mappings\" : { \"_doc\" : {  \"properties\" : {";
        String filed3 = ":{\"type\":\"text\"},";
        String filed4 = ":{\"type\":\"text\"}";
        String filed5 = "}}}}'";
        //获取表名
        ArrayList tables = tableName.getTable();
        Iterator iterator = tables.iterator();
        while (iterator.hasNext()) {
            String table = iterator.next().toString();
            //小写表名
            String tableLower = table.toLowerCase();
            ArrayList cols = tableName.getTableColumnName(table);

            StringBuilder sb = new StringBuilder();
            Iterator it = cols.iterator();
            sb.append(filed1);
            sb.append(tableLower);
            sb.append(filed2);
            for (int i = 0; i < cols.size(); i++) {
                String col = cols.get(i).toString().toLowerCase();
                String colAll = "\"" + col + "\"";
                String lastCol = cols.get(cols.size() - 1).toString().toLowerCase();

                if (col.equals(lastCol)) {
                    sb.append(colAll);
                    sb.append(filed4);
                } else {
                    sb.append(colAll);
                    sb.append(filed3);
                }
            }

            sb.append(filed5);
            System.out.println(sb + "\r\n");
        }
        System.out.println("done!=====================================================>");

    }
}

