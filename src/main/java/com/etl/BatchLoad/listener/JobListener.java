package com.etl.BatchLoad.listener;

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

import com.etl.BatchLoad.comm.BatchEtlLog;
import com.etl.BatchLoad.comm.CacheInfoDAO;
import com.etl.BatchLoad.comm.CacheInfoParser;

public class JobListener implements JobExecutionListener{
	 @Autowired
	 private CacheManager cacheManager;
	 @Autowired
	 private JdbcTemplate jdbcTemplate;
	 
	 @Autowired
	 private BatchEtlLog etllog;
	 
	 private String jobName;
	 private boolean isExistRef = false;
	 
	    public JobListener(String jobName)
	    {
	    	this.jobName = jobName;
	    	
	    }
	    
	    public void isCacheTable(boolean flag)
	    {
	    	this.isExistRef = flag;
	    }
	    
	 
	    @Override
	    public void beforeJob(JobExecution jobExecution) {
	       
	    	//System.out.println("===Start JOB["+jobName+"] execute===");
	    	String jobseq = new SimpleDateFormat("yyyyMMddHHmmss").format(new Timestamp(System.currentTimeMillis()));
	    	cacheManager.getCache(jobName+"_JOBSEQ").put("JOBSEQ", jobseq);
	    	cacheManager.getCache(jobName+"_STATUS").put("STATUS", "OK");
	    	etllog.initialLog(jobseq, jobName);
	    	
	    	
	    	//判斷是否執行cache reference table
	    	if(isExistRef)
	    	{
	    		List<CacheInfoDAO> parsers = CacheInfoParser.getRefInfo(jobName);
	    		
	    		for(int i=0;i<parsers.size();i++)
	    		{
	    			System.out.println("====START CACHE NAME ==="+parsers.get(i).getCacheName());
	    			cacheRefTable(parsers.get(i).getCacheName(),parsers.get(i).getSql(),
	    					parsers.get(i).getKeycolumn(),parsers.get(i).getValuecolumn()
	    					);
	    			
	    		}
	    		
	    	}
	    	
	    	
	       
	    }

	    @Override
	    public void afterJob(JobExecution jobExecution) {
	        // clear cache when the job is finished
	    	if(isExistRef)
	    	{

		    	List<CacheInfoDAO> parsers = CacheInfoParser.getRefInfo(jobName);
		    	for(int i=0;i<parsers.size();i++)
		    	{
		    		releaseCacheRefTable(parsers.get(i).getCacheName());
		    	}
	    		
	    	}
	    	
	    	int totalcnt = (int) cacheManager.getCache(jobName+"_LOG").get("TOTALCNT").get();
	    	int failcnt = (int) cacheManager.getCache(jobName+"_LOG").get("FAILCNT").get();
	    	int writecnt = (int) cacheManager.getCache(jobName+"_LOG").get("WRITECNT").get();
	    	String jobseq = (String) cacheManager.getCache(jobName+"_JOBSEQ").get("JOBSEQ").get();
	    	String status = (String) cacheManager.getCache(jobName+"_STATUS").get("STATUS").get();
	    	
	    	etllog.updateLog(jobseq, jobName, totalcnt, failcnt, writecnt, 0,0,0,status,"");
            
	    	 
	    	 
	    	 
	    	System.out.println("===Finished JOB["+jobName+"] execute===");
	    	
	    	
	    }
	    
	    
	    public void cacheRefTable(String cacheName,String sql,String keyColumn,String valueColumn)
	    {
	    	List rows = jdbcTemplate.queryForList(
	    	          sql);
	    	    Iterator it = rows.iterator();
	    	    while(it.hasNext()) {
	    	    	Map userMap = (Map) it.next();
	    	    	String key=(String)userMap.get(keyColumn); 
	    	    	String value=(String)userMap.get(valueColumn); 
	    	    	//System.out.println("==cache data===>"+key+","+value);
	    	    	cacheManager.getCache(jobName+"_"+cacheName).put(key, value); 
	    	    
	    	    }
	    }
	    
	    public void releaseCacheRefTable(String cacheName)
	    {

	    	
	    	 cacheManager.getCache(jobName+"_"+cacheName).clear();
	    }
	    
	 
}
