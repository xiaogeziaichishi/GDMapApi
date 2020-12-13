package com.io.bytes.file;

import com.io.FilePath;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author liuguibin
 * @date 2020-12-13 13:58
 * 字节流案例：操作文件流的，直接与OS底层交互。因此他们也被称为节点流。
 * Java7之后，可以在 try() 括号中打开流，最后程序会自动关闭流对象，不再需要显示地close
 * 先读取文件，再写到指定文件
 * error :缓冲会影响乱码，写入中文时候会乱码，但是读取写入是没有影响的
 */
public class FileOutputStreamTest {
    public static void getFileOutputStreamIo() {
        try {
            FileInputStream is = new FileInputStream(FilePath.FILETEXT);
            // true 是追加 ,否则重写
            FileOutputStream os = new FileOutputStream(FilePath.BLANKTEXT,true);
            byte[] buff = new byte[10];
            int hasRead = 0;
            //数据保存buff
            while ((hasRead = is.read(buff)) > 0) {
                os.write(buff,0,hasRead);
                os.write("\r\n".getBytes(StandardCharsets.UTF_8));
                os.write("哈哈哈哈".getBytes(StandardCharsets.UTF_8));
                os.write("\r\n".getBytes(StandardCharsets.UTF_8));

                System.out.println(hasRead);
                System.out.println("完成！");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
