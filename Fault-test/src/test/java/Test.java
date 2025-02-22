import org.springframework.boot.SpringBootConfiguration;
import top.huajieyu001.entity.DeviceFaultRecords;

import java.util.ArrayList;
import java.util.List;

@SpringBootConfiguration
public class Test {

    @org.junit.jupiter.api.Test
    public void test(){

        DeviceFaultRecords r1 = new DeviceFaultRecords();
        DeviceFaultRecords r2 = new DeviceFaultRecords();
        DeviceFaultRecords r3 = new DeviceFaultRecords();

        r1.setOpType(1);
        r2.setOpType(3);
        r3.setOpType(2);

        List<DeviceFaultRecords> recordsList = new ArrayList<>();
        recordsList.add(r1);
        recordsList.add(r2);
        recordsList.add(r3);

        recordsList.sort((a, b) -> a.getOpType() > b.getOpType() ? 1 : 0);

        recordsList.forEach(System.out::println);
    }
}
