package com.etl.BatchLoad.comm;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;


@Component
public class BatchEtlLog {

	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	@Value("${batch.etl.log.table}")
	 private String logtable;
	
	public void insertLog(String tableName,String columnName,String columnValue,int readCnt,int skipCnt,
			             int writeCnt,int sumfieldReadCnt,int sumfieldSkipCnt,
			             int sumfieldWriteCnt,String execTime)
	{
		jdbcTemplate.update("INSERT INTO "+logtable+" (TABLE_NAME,COLUMN_NAME,"
				+ "COLUMN_VALUE,READ_CNT,SKIP_CNT,"
				+ "WRITE_CNT,SUMFIELD_READ_CNT,"
				+ "SUMFIELD_SKIP_CNT,"
				+ "SUMFIELD_WRITE_CNT,EXEC_TIME) VALUES (?,?,?,?,?,?,?,?,?,?) ",
				tableName,columnName,columnValue,readCnt,skipCnt,writeCnt,sumfieldReadCnt,
				sumfieldSkipCnt,sumfieldWriteCnt,execTime
				);
	}
	
	
	
	
}
