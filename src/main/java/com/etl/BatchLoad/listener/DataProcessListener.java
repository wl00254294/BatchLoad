package com.etl.BatchLoad.listener;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;

import com.etl.BatchLoad.comm.BatchEtlLog;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DataProcessListener<T, S> implements ItemProcessListener<T, S>{
	@Autowired
	private CacheManager cacheManager;
	@Autowired
	private BatchEtlLog log;
	
	private String jobName;
	
	public DataProcessListener(String jobName)
	{
		this.jobName = jobName;
	}

	 @Override
	 public void beforeProcess(T item) {
	    
	 }
	 
	 @Override
	 public void afterProcess(T item, S result) {
	    
	 }
	 
	 @Override
	 public void onProcessError(T item, Exception e) {
		 System.out.println("ItemProcessListener - onProcessError");
		 cacheManager.getCache(jobName+"_STATUS").put("STATUS", "FAIL");
		 ObjectMapper objectMapper = new ObjectMapper();
		 try {
			String jsondata = objectMapper.writeValueAsString(item);
			
			String jobseq = (String) cacheManager.getCache(jobName+"_JOBSEQ").get("JOBSEQ").get();
			log.insertDetail(jobseq, jobName, jsondata, "Processor","ERROR", e.toString());
		} catch (JsonProcessingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    
	 }
}
