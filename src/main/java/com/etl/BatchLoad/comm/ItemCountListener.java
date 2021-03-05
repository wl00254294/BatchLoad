package com.etl.BatchLoad.comm;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
public class ItemCountListener implements ChunkListener{
	
	    @Autowired
	    private CacheManager cacheManager;    
	
	    @Override
	    public void beforeChunk(ChunkContext context) {
	    }
	 
	    @Override
	    public void afterChunk(ChunkContext context) {
	         
	        int count = context.getStepContext().getStepExecution().getReadCount();
	        
	        cacheManager.getCache("TOTALCNT").put("TOTALCNT", count); 
	        System.out.println("RederCount: " + count);
	    }
	     
	    @Override
	    public void afterChunkError(ChunkContext context) {
	    }
}
