package com.xxl.job.executor.main;

import javax.sql.DataSource;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;


@SpringBootApplication
@ComponentScan(basePackages="com.xxl.job.executor")
@EnableAsync//开启多线程
@EnableTransactionManagement//开启事务
public class JobExecutorApplication extends SpringBootServletInitializer {

	public static void main(String[] args) {
        SpringApplication.run(JobExecutorApplication.class, args);
	}
	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder application){
		return application.sources(JobExecutorApplication.class);
	}
	@Bean
    public PlatformTransactionManager txManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);

    }
}