package com.etl.BatchLoad.listener;

import java.util.List;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;

public class DataWriteListener<T> implements ItemWriteListener<T> {
	
	@Autowired
	 private CacheManager cacheManager;
	
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
		System.out.println(exception.getMessage());
    }
}
