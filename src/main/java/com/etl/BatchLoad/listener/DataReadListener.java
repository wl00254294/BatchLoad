package com.etl.BatchLoad.listener;

import org.springframework.batch.core.ItemReadListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;

import com.etl.BatchLoad.EricThread.EricThread;



public class DataReadListener<T> implements ItemReadListener<T>  {
	
	 @Autowired
	 private CacheManager cacheManager;
	
	
	private String jobName;
	
	public DataReadListener (String jobName)
	{
		this.jobName = jobName;
	}
	
	@Override
	public void beforeRead() {		
	}

	@Override
	public void afterRead(T item) {
		
		
	}

	@Override
	public void onReadError(Exception ex) {
		cacheManager.getCache(jobName+"_STATUS").put("STATUS", "FAIL");
		System.out.println(ex.getMessage());
	}


}
