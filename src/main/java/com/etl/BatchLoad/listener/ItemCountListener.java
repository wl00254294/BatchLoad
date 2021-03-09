package com.etl.BatchLoad.listener;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
public class ItemCountListener implements ChunkListener{
	

		@Autowired
		private CacheManager cacheManager;
        
        private String jobName;

    	/**
    	 * 
    	 *ItemCountListener
    	 * @param catch name for count listener {@link String}
    	 */
        public ItemCountListener(String jobName)
        {
        	this.jobName = jobName;

        }
        
	    @Override
	    public void beforeChunk(ChunkContext context) {
	    }
	 
	    @Override
	    public void afterChunk(ChunkContext context) {
	    	String timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Timestamp(System.currentTimeMillis()));
	    	
	    	int count = context.getStepContext().getStepExecution().getReadCount();
	        int writecount = context.getStepContext().getStepExecution().getWriteCount();        
	        int failcount =context.getStepContext().getStepExecution().getRollbackCount();

	        
	        System.out.println("count:"+(count+failcount)+",fail:"+failcount+",write:"+writecount);
	        
	        cacheManager.getCache(jobName+"_LOG").put("TOTALCNT", count+failcount); 
	        cacheManager.getCache(jobName+"_LOG").put("FAILCNT", failcount); 
	        cacheManager.getCache(jobName+"_LOG").put("WRITECNT", writecount);

	
	    }
	     
	    @Override
	    public void afterChunkError(ChunkContext context) {
	    	System.out.println("=====afterChunkError");
	    	int count = context.getStepContext().getStepExecution().getReadCount();
	        int failcount = context.getStepContext().getStepExecution().getRollbackCount();
	        int writecount = context.getStepContext().getStepExecution().getWriteCount();
	      
	        cacheManager.getCache(jobName+"_LOG").put("TOTALCNT", count); 
	        cacheManager.getCache(jobName+"_LOG").put("FAILCNT", failcount); 
	        cacheManager.getCache(jobName+"_LOG").put("WRITECNT", writecount);
	        
	        System.out.println("count:"+(count)+",fail:"+failcount+",write:"+writecount);
	        
	    }
}
