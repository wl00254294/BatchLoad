package com.etl.BatchLoad.config;
import javax.sql.DataSource;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class BatchConfig extends DefaultBatchConfigurer{
	 
	 @Value("${spring.datasource.driver-class-name}")
	 private String className;
	 
	 @Value("${spring.datasource.url}")
	 private String url;	 
	 
	 @Value("${spring.datasource.username}")
	 private String userName;	 
	 
	 @Value("${spring.datasource.password}")
	 private String pwd;	 
	 
	 @Override
	  public void setDataSource(DataSource dataSource) {
	    // initialize will use a Map based JobRepository (instead of database)
	  }
	 
	  @Bean
	  public DataSource dataSource() {
	        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
	        dataSourceBuilder.driverClassName(className);
	        dataSourceBuilder.url(url);
	        dataSourceBuilder.username(userName);
	        dataSourceBuilder.password(pwd);
	        return dataSourceBuilder.build();
	  }
}
