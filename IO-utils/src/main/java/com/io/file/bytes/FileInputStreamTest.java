package com.io.file.bytes;

import com.io.FilePath;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * @author liuguibin
 * @date 2020-12-13 13:30
 * 字节流案例：操作文件流的，直接与OS底层交互。因此他们也被称为节点流。
 * Java7之后，可以在 try() 括号中打开流，最后程序会自动关闭流对象，不再需要显示地close
 * 读取文件，输出控制台
 */
public class FileInputStreamTest {
    public static void getFileInputStreamIo() {
        FileInputStream is = null;
        try {
            is = new FileInputStream(FilePath.FILETEXT);
            byte[] buf = new byte[1024];
            int hasRead = 0;
            //read()返回的是单个字节数据（字节数据可以直接专程int类型),
            // 但是read(buf)返回的是读取到的字节数，真正的数据保存在buf中
            // hasRead 存储的是字节数
            while ((hasRead = is.read(buf)) > 0) {
                //42
                System.out.println(hasRead);
                System.out.println(new String(buf, 0, hasRead));
            }
            is.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
