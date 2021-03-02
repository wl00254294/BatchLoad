package com.etl.BatchLoad.EricThread;
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
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.util.StringUtils;

import com.etl.BatchLoad.comm.FlatFileItemReaderBinary;


@EnableBatchProcessing
@Configuration
public class EricThreadConfig {
	 @Autowired
	 DataSource conn;

		
	  @Bean
	  public Job ericJob(JobBuilderFactory jobBuilders,
	      StepBuilderFactory stepBuilders) {
	    return jobBuilders.get("ericJob")
	        .start(ericStep(stepBuilders)).build();
	  }

	  @Bean
	  public Step ericStep(StepBuilderFactory stepBuilders) {
	    return stepBuilders.get("ericStep")
	        .<EricThread, EricThread>chunk(50000).reader(reader2())
	        .processor(processor2())
	        .writer(writer2())
	        .taskExecutor(taskExecutor())
	        .throttleLimit(5)
	        .build();
	  }

	 // 資料換行
	  //@Bean
	 // public FlatFileItemReader<EricThread> reader2() {
//		  FlatFileItemReader<EricThread> itemReader = new FlatFileItemReader<EricThread>();
//		  itemReader.setEncoding("MS950");
//		  itemReader.setLineMapper(lineMapper2());
		
//		  itemReader.setResource(new FileSystemResource("C:\\cr\\EcsWeb\\media\\1\\2.txt"));
		  
//		  return itemReader;
		  
//	  }
	  
	  //資料在同一行
	  @Bean
	  public FlatFileItemReaderBinary<EricThread> reader2() {
		  FlatFileItemReaderBinary<EricThread> itemReader = new FlatFileItemReaderBinary<EricThread>();
		  itemReader.setEncoding("MS950");
		  itemReader.setLineMapper(lineMapper2());
		  itemReader.setRecordLength(53);
		  itemReader.setResource(new FileSystemResource("C:\\cr\\EcsWeb\\media\\1\\2.txt"));
		  
		  return itemReader;
		  
	  }  
	  @Bean
	  public TaskExecutor taskExecutor() {
	      return new SimpleAsyncTaskExecutor("eric_batch");
	  }
	  
	  @Bean
	  public LineMapper<EricThread> lineMapper2() {
	        DefaultLineMapper<EricThread> lineMapper = new DefaultLineMapper<EricThread>();
	        FixedLengthTokenizer lineTokenizer =  fixedLengthTokenizer2();
	        BeanWrapperFieldSetMapper<EricThread> fieldSetMapper = new BeanWrapperFieldSetMapper<EricThread>();
	        fieldSetMapper.setTargetType(EricThread.class);
	        
	        lineMapper.setLineTokenizer(lineTokenizer);
	        lineMapper.setFieldSetMapper(fieldSetMapper);
	        return lineMapper;
	  }	  
	  
	  @Bean
	  public FixedLengthTokenizer fixedLengthTokenizer2() {
	  	FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();

	  	tokenizer.setNames("col1", "col2","col3","col4","col5","col6","col7");
	  	tokenizer.setColumns(new Range(1, 15),
	  						new Range(16, 30),
	  						new Range(31, 34),
	  						new Range(35, 42),
	  						new Range(43, 44),
	  	                    new Range(45, 52),
	  	                    new Range(53, 53)
	  			            );
	  	
	  	tokenizer.setStrict(false);

	  	return tokenizer;
	  }

	  @Bean
	  public EricThreadProcessor processor2() {
	    return new EricThreadProcessor();
	  }


	  @Bean
	  public JdbcBatchItemWriter<EricThread> writer2() {
		  
		  JdbcBatchItemWriter<EricThread> iwt = new JdbcBatchItemWriter<>();
		
	        iwt.setDataSource(conn);
	        iwt.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<EricThread>());
	        //itemWriter.setSql("INSERT INTO ERIC_THREAD (COL1) VALUES (:lastName)");
	        iwt.setSql("INSERT INTO ERIC_THREAD (COL1, COL2,COL3,COL4,COL5,COL6,COL7) VALUES (:col1,:col2,:col3,:col4,:col5,:col6,:col7)");
	        
	        return iwt;
	  }
}
