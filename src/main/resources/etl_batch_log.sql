CREATE TABLE etl_batch_log  (
          "JOB_SEQ"  VARCHAR(50) WITH DEFAULT '' ,
		  "JOB_NAME" VARCHAR(50) WITH DEFAULT '' , 
		  "READ_CNT" INTEGER WITH DEFAULT 0 , 
		  "SKIP_CNT" INTEGER WITH DEFAULT 0 , 
		  "WRITE_CNT" INTEGER WITH DEFAULT 0 , 
		  "SUMFIELD_READ_CNT" DOUBLE WITH DEFAULT 0 , 
		  "SUMFIELD_SKIP_CNT" DOUBLE WITH DEFAULT 0 , 
		  "SUMFIELD_WRITE_CNT" DOUBLE WITH DEFAULT 0 ,
          "JOB_STATUS" VARCHAR(20) WITH DEFAULT '',
          "MSG"	VARCHAR(500) WITH DEFAULT '',	  
		  "START_TIME" TIMESTAMP,
		  "ELAPSED_TIME" double,
		  "END_TIME" TIMESTAMP
		  );