package com.io.chars.transffrom;

import com.io.FilePath;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * @author liuguibin
 * @date 2020-12-13 15:00
 * InputStreamReader 是字节流通向字符流的桥梁：它使用指定的 charset 读取字节并将其解码为字符。
 * OutputStreamWriter 是字符流通向字节流的桥梁：它使用指定的 charset 将要写入流中的字符编码成字节。
 */
public class InputStreamReaderTest {
    private InputStreamReaderTest() {
        try {
            throw new IllegalAccessException("Utility class");
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public static void getInputStreamReaderIo() {
        FileInputStream fis = null;
        InputStreamReader is = null;
        try {
            fis = new FileInputStream(FilePath.FILETEXT);
            is = new InputStreamReader(fis, StandardCharsets.UTF_8);
            char[] chs = new char[20];
            int len;
            while ((len = is.read(chs)) != -1) {
                System.out.println(len);
                System.out.println(new String(chs, 0, len));
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e2) {
                    e2.printStackTrace();
                }
            }

        }
    }
}
