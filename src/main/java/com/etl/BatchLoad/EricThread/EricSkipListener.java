package com.etl.BatchLoad.EricThread;
import org.springframework.batch.core.SkipListener;

//開發中
public class EricSkipListener implements SkipListener<EricThread, EricThread>{
	@Override
    public void onSkipInRead(Throwable t) {
        System.out.println("StepSkipListener - onSkipInRead");
    }
 
    @Override
    public void onSkipInWrite(EricThread item, Throwable t) {
        System.out.println("StepSkipListener - afterWrite");
    }
 
    @Override
    public void onSkipInProcess(EricThread item, Throwable t) {
        System.out.println("StepSkipListener - onWriteError");
    }
}
