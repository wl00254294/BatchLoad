package com.etl.BatchLoad.comm;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.jdbc.core.JdbcTemplate;

public class JobListener implements JobExecutionListener{
	 @Autowired
	 private CacheManager cacheManager;
	 @Autowired
	 private JdbcTemplate jdbcTemplate;
	 
	 @Autowired
	 private BatchEtlLog etllog;
	 
	 private String jobName;
	 private String isExistRef;
	 
	 private String cacheName;
	 private String sql;
	 private String keyColumn;
	 
	    public JobListener(String jobName,String isExistRef)
	    {
	    	this.jobName = jobName;
	    	this.isExistRef = isExistRef;
	    }
	 
	    @Override
	    public void beforeJob(JobExecution jobExecution) {
	       
	    	System.out.println("===Start JOB["+jobName+"] execute===");
	    	String jobseq = new SimpleDateFormat("yyyyMMddHHmmss").format(new Timestamp(System.currentTimeMillis()));
	    	cacheManager.getCache(jobName+"_JOBSEQ").put("JOBSEQ", jobseq);
	    	
	    	etllog.initialLog(jobseq, jobName);
	    	
	    	
	    	//判斷是否執行cache reference table
	    	if(isExistRef.equals("Y"))
	    	{
	    		if(cacheName == null || sql == null || keyColumn == null)
	    		{
	    			System.out.println("cacheName,sql,keyColumn must be set");
	    			return;
	    		}
	    		cacheRefTable(cacheName,sql,keyColumn);
	    	}
	    	
	    	
	       
	    }

	    @Override
	    public void afterJob(JobExecution jobExecution) {
	        // clear cache when the job is finished
	    	if(isExistRef.equals("Y"))
	    	{
	    	  if(cacheName == null)
	    	  {
	    		  System.out.println("Parameter cacheName must  set");
	    		  return;
	    	  }
	    		releaseCacheRefTable(cacheName);
	    	}
	    	
	    	int totalcnt = (int) cacheManager.getCache("ERIC_THREAD_LOG").get("TOTALCNT").get();
	    	int failcnt = (int) cacheManager.getCache("ERIC_THREAD_LOG").get("FAILCNT").get();
	    	int writecnt = (int) cacheManager.getCache("ERIC_THREAD_LOG").get("WRITECNT").get();
	    	String jobseq = (String) cacheManager.getCache(jobName+"_JOBSEQ").get("JOBSEQ").get();
	    	
	    	etllog.updateLog(jobseq, jobName, totalcnt, failcnt, writecnt, 0,0,0,"FINISH","");
            
	    	 
	    	 
	    	 
	    	System.out.println("===Finished JOB["+jobName+"] execute===");
	    	
	    	
	    }
	    
	    public void setCacheRefTableInfo(String cacheName,String sql,String keyColumn)
	    {
	    	this.cacheName = cacheName;
	    	this.sql = sql;
	    	this.keyColumn = keyColumn;
	    }
	    
	    public void cacheRefTable(String cacheName,String sql,String keyColumn)
	    {
	    	List rows = jdbcTemplate.queryForList(
	    	          sql);
	    	    Iterator it = rows.iterator();
	    	    while(it.hasNext()) {
	    	    	Map userMap = (Map) it.next();
	    	    	String key=(String)userMap.get(keyColumn);     
	    	    	//System.out.println("==cache data===>"+key);
	    	    	cacheManager.getCache(jobName+"_"+cacheName).put(key, "Y"); 
	    	    
	    	    }
	    }
	    
	    public void releaseCacheRefTable(String cacheName)
	    {
	    	 cacheManager.getCache(jobName+"_"+cacheName).clear();
	    }
	    
	 
}
