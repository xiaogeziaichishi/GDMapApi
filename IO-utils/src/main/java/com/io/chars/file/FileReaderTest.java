package com.io.chars.file;

import com.io.FilePath;

import java.io.FileReader;
import java.io.IOException;

/**
 * @author liuguibin
 * @date 2020-12-13 14:42
 */
public class FileReaderTest {
    public static void FileReaderIo() {
        try {
            FileReader fr = new FileReader(FilePath.FILETEXT);
            char[] buf = new char[32];
            int hasRead = 0;
            // 每个char都占两个字节，每个字符或者汉字都是占2个字节，因此无论buf长度为多少，总是能读取中文字符长度的整数倍,不会乱码
            while ((hasRead = fr.read(buf)) > 0) {
                System.out.println(new String(buf, 0, hasRead));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
