package com.etl.BatchLoad.EricThread;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.jdbc.core.JdbcTemplate;

public class EricThreadListener implements JobExecutionListener{
    @Autowired
	private CacheManager cacheManager;
    
    @Autowired
    private JdbcTemplate jdbcTemplate;
    
    @Override
    public void beforeJob(JobExecution jobExecution) {
       
    	System.out.println("===before job execute===");
    	cacheDbaAcno();
    	
       
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        // clear cache when the job is finished
    	System.out.println("===after job execute===");
    	releaseCacheDbaAcno();
    }
    
    public void cacheDbaAcno()
    {
    	List rows = jdbcTemplate.queryForList(
  	          "SELECT ACCT_NO FROM DBA_ACNO");
  	    Iterator it = rows.iterator();
  	    while(it.hasNext()) {
  	    	Map userMap = (Map) it.next();
  	    	String key=(String)userMap.get("ACCT_NO");      
  	    	cacheManager.getCache("DBA_ACNO").put(key, "Y"); 
  	    	System.out.println("=====put DBA_ACNO's ACCT_NO column into cache :"+key);
  	    }
    }
    
    public void releaseCacheDbaAcno()
    {
    	 cacheManager.getCache("DBA_ACNO").clear();
    }
    
}
