package com.etl.BatchLoad.EricThread;
import org.springframework.batch.item.ItemWriter;
import org.springframework.classify.Classifier;

/*
 * 分類器選擇:依條件選擇不同的writer
 * 
 */

public class EricThreadClassifier implements Classifier<EricThread, ItemWriter>{
	private ItemWriter insertThread1;
    private ItemWriter insertThread2;
 
    public EricThreadClassifier(ItemWriter insertThread1, ItemWriter insertThread2) {
        this.insertThread1 = insertThread1;
        this.insertThread2 = insertThread2;
    }
 
    @Override
    public ItemWriter classify(EricThread data) {
        if(data.getCol1().substring(0,1).equals("0"))
        {
        	return insertThread1;
        }
    	
        return insertThread2;
    }
}
