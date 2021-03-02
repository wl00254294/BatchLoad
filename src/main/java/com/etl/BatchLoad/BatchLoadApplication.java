package com.etl.BatchLoad;

import java.util.Date;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.BeansException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.ConfigurableApplicationContext;


@SpringBootApplication
@ComponentScan
@EnableAutoConfiguration
public class BatchLoadApplication {

	public static void main(String[] args) throws BeansException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		SpringApplication application = new SpringApplication(BatchLoadApplication.class);
		ConfigurableApplicationContext ctx = application.run(args);
        JobLauncher jobLauncher = ctx.getBean(JobLauncher.class);
        
        JobParameters jobParameters = new JobParametersBuilder()
                .addDate("date", new Date())
                .toJobParameters();
        //jobLauncher.run(ctx.getBean("personJob", Job.class), jobParameters);
        
        //1 min 19 sec
        jobLauncher.run(ctx.getBean("ericJob", Job.class), jobParameters);

	}

}
