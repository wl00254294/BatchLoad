package com.etl.BatchLoad.comm;

public class CacheInfoDAO {
  public String cacheName;	
  public String sql;
  public String keycolumn;
  public String valuecolumn;
  
  
  
public String getCacheName() {
	return cacheName;
}
public void setCacheName(String cacheName) {
	this.cacheName = cacheName;
}
public String getSql() {
	return sql;
}
public void setSql(String sql) {
	this.sql = sql;
}
public String getKeycolumn() {
	return keycolumn;
}
public void setKeycolumn(String keycolumn) {
	this.keycolumn = keycolumn;
}
public String getValuecolumn() {
	return valuecolumn;
}
public void setValuecolumn(String valuecolumn) {
	this.valuecolumn = valuecolumn;
}

  
  
}
