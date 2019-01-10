package com.half.nock.springbootstartup;

import com.half.nock.quartz.spring.quartz.SchedulerFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;
import java.io.IOException;

@SpringBootApplication
public class SpringbootStartupApplication {


	public static void main(String[] args) {
		SpringApplication.run(SpringbootStartupApplication.class, args);
	}

	@Bean(name = "quartzScheduler")
	public SchedulerFactoryBean schedulerFactoryBean(DataSource dataSource) throws IOException {
		SchedulerFactoryBean factory = new SchedulerFactoryBean();
		factory.setDataSource(dataSource);
		factory.setConfigLocation(new ClassPathResource("/quartz.properties"));
		factory.setApplicationContextSchedulerContextKey("applicationContextKey");
		factory.setAutoStartup(true);
		factory.setStartupDelay(20);
		factory.setOverwriteExistingJobs(true);
		return factory;
	}

}

