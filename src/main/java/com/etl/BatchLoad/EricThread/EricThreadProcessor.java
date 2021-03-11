package com.etl.BatchLoad.EricThread;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache.ValueWrapper;
import org.springframework.cache.CacheManager;





public class EricThreadProcessor implements ItemProcessor<EricThread, EricThread>
{
	@Autowired
	private CacheManager cacheManager;
	
	  @Override
	  public EricThread process(EricThread data) throws Exception {
		  
		  System.out.println(data.getCol1()+"<========Process===");
		  
		  //ValueWrapper key = cacheManager.getCache("ERIC_THREAD_REF").get(data.getCol1());
		  EricThread outData = new EricThread();
		 // if(key != null)
		  //{
			//  System.out.println("found key");
			  
			 
		  //}else {
			 
		//	  System.out.println("not found key");
		 // }
		  outData.setCol1(data.getCol1());
		  outData.setCol2(data.getCol2());
		  outData.setCol3(data.getCol3());
		  outData.setCol4(data.getCol4());
		  outData.setCol5(data.getCol5());
		  outData.setCol6(data.getCol6());
		  if(data.getCol1().equals("1110320095"))
		  {
		 
		  outData.setCol7("I");
		  }//else if(data.getCol1().equals("3110320095"))
		 // {
		//	  System.out.println("=====process error test===");
		//	  int[] i = {1,2};
		//		int k = i[2];
		 // }
		  else {
			  outData.setCol7(data.getCol7());
		  }
          
	    return outData;
	  }
}
