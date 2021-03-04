package com.etl.BatchLoad.EricThread;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.cache.CacheManager;





public class EricThreadProcessor implements ItemProcessor<EricThread, EricThread>
{
	@Autowired
	private CacheManager cacheManager;
	
	private int cnt=0;
	  @Override
	  public EricThread process(EricThread data) throws Exception {
		  ValueWrapper key = cacheManager.getCache("DBA_ACNO").get(data.getCol1());
		  
		  if(key != null)
		  {
			  System.out.println("found key");
		  }else {
			  System.out.println("not found key");
		  }
		  
		  EricThread outData = new EricThread();
		  outData.setCol1(data.getCol1());
		  outData.setCol2(data.getCol2());
		  outData.setCol3(data.getCol3());
		  outData.setCol4(data.getCol4());
		  outData.setCol5(data.getCol5());
		  outData.setCol6(data.getCol6());
		  outData.setCol7(data.getCol7());
	      cnt++; 
          System.out.println(data.getCol1()+"<========Process=>"+cnt+"==");
	    return outData;
	  }
}
