package com.etl.BatchLoad.comm;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

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
	
	@Value("${batch.etl.detail.table}")
	private String detailtable;
	
	public void insertDetail(String jobseq,String jobname
			,String data,String step,String type,String msg)
	{
		jdbcTemplate.update("INSERT INTO "+detailtable+" (JOB_SEQ,"
				+ "JOB_NAME,"
				+ "DATA,"
				+ "STEP,"
				+ "TYPE,"
				+ "MSG,"
				+ "EVENT_TIME) VALUES (?,?,?,?,?,?,current_timestamp) ",
				jobseq,jobname,data,step,type,msg
				);
	}
	
	
	public void insertLog(String jobseq,String jobname,int readcnt,int skipcnt ,
			             int writecnt,int sumfieldreadcnt,int sumfieldskipcnt,
			             int sumfieldwritecnt,String jobstatus,String msg,Timestamp starttime,Time elapsedtime
			             ,Timestamp endtime)
	{
		jdbcTemplate.update("INSERT INTO "+logtable+" (JOB_SEQ,"
				+ "JOB_NAME,READ_CNT,SKIP_CNT,WRITE_CNT,"
				+ "SUMFIELD_READ_CNT,SUMFIELD_SKIP_CNT,SUMFIELD_WRITE_CNT,"
				+ "JOB_STATUS,MSG,START_TIME,ELAPSED_TIME,"
				+ "END_TIME) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ",
				jobseq,jobname,readcnt,skipcnt,
				writecnt,sumfieldreadcnt,sumfieldskipcnt,
				sumfieldwritecnt,jobstatus,msg,starttime,elapsedtime
				,endtime
				);
	}
	
	public void updateLog(String jobseq,String jobname,int readcnt,int skipcnt ,
			             int writecnt,int sumfieldreadcnt,int sumfieldskipcnt,
			             int sumfieldwritecnt,String jobstatus,String msg)
	{
		jdbcTemplate.update("UPDATE "+logtable+" SET READ_CNT = ?, "
				+"SKIP_CNT = ?,"
				+ "WRITE_CNT = ?,"
				+ "SUMFIELD_READ_CNT = ?,"
				+ "SUMFIELD_SKIP_CNT = ?,"
				+ "SUMFIELD_WRITE_CNT = ?,"
				+ "JOB_STATUS = ?,"
				+ "MSG = ?,"
				+ "END_TIME= CURRENT_TIMESTAMP,"
				+ "ELAPSED_TIME = CURRENT_TIMESTAMP - START_TIME"
				+ " WHERE JOB_SEQ = ? and JOB_NAME = ? "
				,readcnt,skipcnt,writecnt,
				sumfieldreadcnt,sumfieldskipcnt,
				sumfieldwritecnt,jobstatus,msg,jobseq,jobname
				);
		
	}
	
	public void initialLog(String jobseq,String jobname)
	{
		jdbcTemplate.update("INSERT INTO "+logtable+" (JOB_SEQ,"
				+ "JOB_NAME,"
				+ "JOB_STATUS,START_TIME) VALUES (?,?,'START',CURRENT_TIMESTAMP) ",
				jobseq,jobname
				);
	}
	

	
	
	
	
	
	
}
