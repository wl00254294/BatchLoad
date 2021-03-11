package com.etl.BatchLoad.listener;

import org.springframework.batch.core.ItemReadListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;

import com.etl.BatchLoad.EricThread.EricThread;
import com.etl.BatchLoad.comm.BatchEtlLog;



public class DataReadListener<T> implements ItemReadListener<T>  {
	
	 @Autowired
	 private CacheManager cacheManager;
     @Autowired
	 private BatchEtlLog log;	
	
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
		
		String jobseq = (String) cacheManager.getCache(jobName+"_JOBSEQ").get("JOBSEQ").get();
		log.insertDetail(jobseq, jobName, "", "Reader","ERROR", ex.toString());
		System.out.println(ex.getMessage());
	}


}
