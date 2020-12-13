package com.io.file.chars;

import com.io.FilePath;

import java.io.FileWriter;
import java.io.IOException;

/**
 * @author liuguibin
 * @date 2020-12-13 14:28
 */
public class FileWriterTest {
    public static void FileWriterIo(){
        try {
            // true 是追加 ,否则重写
            FileWriter fw = new FileWriter(FilePath.BLANKTEXT,true);
            fw.write("张三\r\n");
            fw.write("李四\r\n");
            System.out.println("完成！");
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
