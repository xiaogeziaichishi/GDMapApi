import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @author liuguibin
 * @date 2021-01-06 09:39
 */
public class CommandUtil {
    protected static final Logger logger = LoggerFactory.getLogger(CommandUtil.class);

    public static String executeLinuxCmd(String cmd) {
        Runtime runtime = Runtime.getRuntime();
        try {
            Process process = runtime.exec(cmd);
            InputStream in = process.getInputStream();
            BufferedReader bs = new BufferedReader(new InputStreamReader(in));
            StringBuffer out = new StringBuffer();
            byte[] b = new byte[8192];
            for (int n; (n = in.read(b)) != -1; ) {
                out.append(new String(b, 0, n));
            }
            System.out.println(out.toString());
            in.close();
            process.destroy();
        } catch (IOException e) {
            logger.info("IO流异常");
            e.printStackTrace();
        }
        return null;
    }
}
