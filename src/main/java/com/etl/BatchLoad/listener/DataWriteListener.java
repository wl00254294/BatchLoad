package com.etl.BatchLoad.listener;

import java.util.List;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;

import com.etl.BatchLoad.comm.BatchEtlLog;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DataWriteListener<T> implements ItemWriteListener<T> {
	
	@Autowired
	 private CacheManager cacheManager;
	@Autowired
	private BatchEtlLog log;
	
	private String jobName;
	
	public DataWriteListener(String jobName)
	{
		this.jobName = jobName;
	}
	
	@Override
    public void beforeWrite(List<? extends T> items) {
        //System.out.println("ItemWriteListener - beforeWrite");
    }
 
    @Override
    public void afterWrite(List<? extends T> items) {
       // System.out.println("ItemWriteListener - afterWrite");
    }
 
    @Override
    public void onWriteError(Exception exception, List<? extends T> items) {
    
    	cacheManager.getCache(jobName+"_STATUS").put("STATUS", "FAIL");
    	
    	for(int i=0;i<items.size();i++)
    	{
    		ObjectMapper objectMapper = new ObjectMapper();
   		    try {
   		    	String jsondata = objectMapper.writeValueAsString(items.get(i));
 			
   		    	String jobseq = (String) cacheManager.getCache(jobName+"_JOBSEQ").get("JOBSEQ").get();
 			    log.insertDetail(jobseq, jobName, jsondata, "Writer","ERROR", exception.toString());
   		    } catch (JsonProcessingException e1) {
   		    	// TODO Auto-generated catch block
   		    	e1.printStackTrace();
   		    }
    	}
    	
    	
		System.out.println(exception.getMessage());
    }
}
