import com.test.AddressLngLatExchange;
import com.test.SerachLngLat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author liuguibin
 * @date 2020-12-12 13:58
 */
public class test {
    protected static final Logger logger = LoggerFactory.getLogger(test.class);
    public static void main(String[] args) {
        Map<Double, Double> bj = SerachLngLat.getValues("北京市朝阳区阜通东大街6号");
        for(Map.Entry<Double, Double> entry : bj.entrySet()){
            Double mapKey = entry.getKey();
            Double mapValue = entry.getValue();
            System.out.println(mapKey+":"+mapValue);
        }

    }
}
