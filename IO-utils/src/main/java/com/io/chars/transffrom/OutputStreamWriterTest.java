package com.io.chars.transffrom;

import com.io.FilePath;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * @author liuguibin
 * @date 2020-12-13 15:19
 * InputStreamReader 是字节流通向字符流的桥梁：它使用指定的 charset 读取字节并将其解码为字符。
 * OutputStreamWriter 是字符流通向字节流的桥梁：它使用指定的 charset 将要写入流中的字符编码成字节。
 */
public class OutputStreamWriterTest {
    public static void getInputStreamReaderIo(){
        OutputStreamWriter os = null;
        InputStreamReader is = null;
        try {
             os = new OutputStreamWriter(new FileOutputStream(FilePath.BLANKTEXT),StandardCharsets.UTF_8);
             is = new InputStreamReader(new FileInputStream(FilePath.FILETEXT), StandardCharsets.UTF_8);
            char[] chs = new char[20];
            int len;
            while ((len = is.read(chs))!=-1){
                os.write(chs,0,len);
                os.flush();
            }
            os.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
           if(os!= null){
               try {
                   os.close();
               } catch (IOException e1) {
                   e1.printStackTrace();
               }
           }

           if (is!=null){
               try {
                   is.close();
               } catch (IOException e2) {
                   e2.printStackTrace();
               }
           }

        }
    }
}
