package com.etl.BatchLoad.comm;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
public class ItemCountListener implements ChunkListener{
	

		@Autowired
		private CacheManager cacheManager;
        
        private String cachName;

    	/**
    	 * 
    	 *ItemCountListener
    	 * @param catch name for count listener {@link String}
    	 */
        public ItemCountListener(String cachName)
        {
        	this.cachName = cachName;

        }
        
	    @Override
	    public void beforeChunk(ChunkContext context) {
	    }
	 
	    @Override
	    public void afterChunk(ChunkContext context) {
	    	String timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Timestamp(System.currentTimeMillis()));
	    	
	    	int count = context.getStepContext().getStepExecution().getReadCount();
	        int failcount = context.getStepContext().getStepExecution().getFilterCount();
	        int writecount = context.getStepContext().getStepExecution().getWriteCount();
	        
	        cacheManager.getCache(cachName).put("TOTALCNT", count+failcount); 
	        cacheManager.getCache(cachName).put("FAILCNT", failcount); 
	        cacheManager.getCache(cachName).put("WRITECNT", writecount);

	
	    }
	     
	    @Override
	    public void afterChunkError(ChunkContext context) {
	    
	    }
}
