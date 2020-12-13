package com.io.chars.buffer;

import com.io.FilePath;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * @author liuguibin
 * @date 2020-12-13 16:13
 * 之所以用BufferedReader,而不是直接用BufferedInputStream读取,是因为BufferedInputStream是InputStream的间接子类,
 * InputStream的read方法读取的是一个byte,而一个中文占两个byte,所以可能会出现读到半个汉字的情况,就是乱码.
 * BufferedReader继承自Reader,该类的read方法读取的是char,所以无论如何不会出现读个半个汉字的.
 */
public class BufferedReaderWrite {
    public static void getBufferedReaderWriteIo() {
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        BufferedReader reader = null;
        BufferedWriter bw = null;
        try {
            fis = new FileInputStream(FilePath.FILETEXT);
            bis = new BufferedInputStream(fis);
            //InputStreamReader 字节转字符,指定编码
            reader = new BufferedReader(new InputStreamReader(bis, StandardCharsets.UTF_8));
             bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(FilePath.BLANKTEXT,true), StandardCharsets.UTF_8));

            String str = null;
            while ((str = reader.readLine()) != null) {
                bw.write(str);
                bw.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
