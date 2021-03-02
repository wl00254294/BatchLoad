package com.etl.BatchLoad.Person;
import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;



@EnableBatchProcessing
@Configuration
public class PersonConfig {
	
	 @Autowired
	 DataSource conn;
	
	  @Bean
	  public Job personJob(JobBuilderFactory jobBuilders,
	      StepBuilderFactory stepBuilders) {
	    return jobBuilders.get("personJob")
	        .start(personStep(stepBuilders)).build();
	  }

	  @Bean
	  public Step personStep(StepBuilderFactory stepBuilders) {
	    return stepBuilders.get("personStep")
	        .<Person, Person>chunk(10).reader(reader())
	        .processor(processor())
	        .writer(pwriter()).build();
	  }

	  @Bean
	  public FlatFileItemReader<Person> reader() {
		  FlatFileItemReader<Person> itemReader = new FlatFileItemReader<Person>();
		  itemReader.setLineMapper(lineMapper());
		  itemReader.setResource(new FileSystemResource("C:\\cr\\EcsWeb\\media\\1\\eric.txt"));
		  return itemReader;
		  
	  }
	  
	  @Bean
	  public LineMapper<Person> lineMapper() {
	        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<Person>();
	        FixedLengthTokenizer lineTokenizer =  fixedLengthTokenizer();
	        BeanWrapperFieldSetMapper<Person> fieldSetMapper = new BeanWrapperFieldSetMapper<Person>();
	        fieldSetMapper.setTargetType(Person.class);
	        lineMapper.setLineTokenizer(lineTokenizer);
	        lineMapper.setFieldSetMapper(fieldSetMapper);
	        return lineMapper;
	  }	  
	  
	  @Bean
	  public FixedLengthTokenizer fixedLengthTokenizer() {
	  	FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();

	  	tokenizer.setNames("firstName", "lastName");
	  	tokenizer.setColumns(new Range(1, 4),
	  						new Range(5, 8));
	  	tokenizer.setStrict(false);

	  	return tokenizer;
	  }

	  @Bean
	  public PersonProcessor processor() {
	    return new PersonProcessor();
	  }


	  @Bean
	  public JdbcBatchItemWriter<Person> pwriter() {
	        JdbcBatchItemWriter<Person> itemWriter = new JdbcBatchItemWriter<>();
	        
	        itemWriter.setDataSource(conn);
	        itemWriter.setSql("INSERT INTO PERSON (FIRSTNAME, LASTNAME) VALUES ( :firstName, :lastName)");
	        itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Person>());
	        return itemWriter;
	  }
	     

}
