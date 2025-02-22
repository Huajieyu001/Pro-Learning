package top.huajieyu001;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("top.huajieyu001.mapper")
public class FaultApplication {

    public static void main(String[] args) {
        SpringApplication.run(FaultApplication.class, args);
    }
}
