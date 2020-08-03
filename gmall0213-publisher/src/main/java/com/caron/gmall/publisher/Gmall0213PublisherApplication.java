package com.caron.gmall.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.caron.gmall.publisher.mapper")
public class Gmall0213PublisherApplication {
    public static void main(String[] args) {
        SpringApplication.run(Gmall0213PublisherApplication.class, args);
    }
}
//https://github.com/windyzj/gmall0213-parent