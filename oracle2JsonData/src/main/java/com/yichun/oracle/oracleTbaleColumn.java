package com.yichun.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

public class oracleTbaleColumn {
    public static ArrayList getOracleData() throws SQLException, ClassNotFoundException {
        ArrayList tables = tableName.getTable();
        Iterator iterator = tables.iterator();
        ArrayList<String> list1 = new ArrayList<>();
        while (iterator.hasNext()) {
            //每一行数据,表名
            Object next = iterator.next();
            ArrayList tableColumnName = tableName.getTableColumnName(next.toString());
            Iterator it = tableColumnName.iterator();
            while (it.hasNext()) {
                Object ne = it.next();
                //next 是表名，ne是字段
                //System.out.println(next.toString() + "--" + ne.toString());
                list1.add(next.toString() + "," + ne.toString());
            }
        }
        return list1;
    }
}
