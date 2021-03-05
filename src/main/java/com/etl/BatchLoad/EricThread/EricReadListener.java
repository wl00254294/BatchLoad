package com.etl.BatchLoad.EricThread;

import org.springframework.batch.core.ItemReadListener;

public class EricReadListener implements ItemReadListener<EricThread> {
	@Override
	public void beforeRead() {
		
	}

	@Override
	public void afterRead(EricThread item) {
		
	}

	@Override
	public void onReadError(Exception ex) {
		System.out.println("Eric read Error");
	}

}
