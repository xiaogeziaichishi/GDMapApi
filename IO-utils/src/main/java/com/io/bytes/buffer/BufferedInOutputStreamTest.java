package com.io.bytes.buffer;

import com.io.FilePath;

import java.io.*;


/**
 * @author liuguibin
 * @date 2020-12-13 15:35
 * 也会乱码
 */
public class BufferedInOutputStreamTest {
    public static void getBufferedInputStreamIo() {
        BufferedInputStream bis = null;
        BufferedOutputStream bos = null;
        try {
            bis = new BufferedInputStream(new FileInputStream(FilePath.FILETEXT),10);
            bos = new BufferedOutputStream(new FileOutputStream(FilePath.BLANKTEXT,true),10);
            int len = 0;
            byte[] buff = new byte[16];
            while ((len = bis.read(buff)) != -1) {
                bos.write(buff, 0, len);
                bos.write("哈哈哈".getBytes());
                bos.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bos != null) {
                try {
                    bos.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }

            }
            if (bis != null) {
                try {
                    bis.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }

            }
        }
    }
}
