package com.etl.BatchLoad.config;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.support.ApplicationContextFactory;
import org.springframework.batch.core.configuration.support.GenericApplicationContextFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.etl.BatchLoad.EricThread.EricThreadConfig;
import com.etl.BatchLoad.Person.PersonConfig;


@Configuration
@EnableBatchProcessing(modular=true)
public class JobConfiguration {
	
	//不需要
	@Bean
	public ApplicationContextFactory personJob2(){
	   return new GenericApplicationContextFactory(PersonConfig.class);
	}

	@Bean
	public ApplicationContextFactory ericJob2(){
	   return new GenericApplicationContextFactory(EricThreadConfig.class);
	}

}
