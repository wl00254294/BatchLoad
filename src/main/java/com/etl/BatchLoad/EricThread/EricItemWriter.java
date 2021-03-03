package com.etl.BatchLoad.EricThread;


import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.ItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcParameterUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.util.Assert;


public class EricItemWriter<T> implements ItemWriter<T>, InitializingBean {

	protected static final Log logger = LogFactory.getLog(EricItemWriter.class);

	protected NamedParameterJdbcOperations namedParameterJdbcTemplate;

	protected ItemPreparedStatementSetter<T> itemPreparedStatementSetter;

	protected ItemSqlParameterSourceProvider<T> itemSqlParameterSourceProvider;

	protected String sql;

	protected boolean assertUpdates = true;

	protected int parameterCount;

	protected boolean usingNamedParameters;


	public void setAssertUpdates(boolean assertUpdates) {
		this.assertUpdates = assertUpdates;
	}


	public void setSql(String sql) {
		this.sql = sql;
	}


	public void setItemPreparedStatementSetter(ItemPreparedStatementSetter<T> preparedStatementSetter) {
		this.itemPreparedStatementSetter = preparedStatementSetter;
	}


	public void setItemSqlParameterSourceProvider(ItemSqlParameterSourceProvider<T> itemSqlParameterSourceProvider) {
		this.itemSqlParameterSourceProvider = itemSqlParameterSourceProvider;
	}


	public void setDataSource(DataSource dataSource) {
		if (namedParameterJdbcTemplate == null) {
			this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
		}
	}

	/**
	 * Public setter for the {@link NamedParameterJdbcOperations}.
	 * @param namedParameterJdbcTemplate the {@link NamedParameterJdbcOperations} to set
	 */
	public void setJdbcTemplate(NamedParameterJdbcOperations namedParameterJdbcTemplate) {
		this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
	}

	/**
	 * Check mandatory properties - there must be a SimpleJdbcTemplate and an SQL statement plus a
	 * parameter source.
	 */
	@Override
	public void afterPropertiesSet() {
		Assert.notNull(namedParameterJdbcTemplate, "A DataSource or a NamedParameterJdbcTemplate is required.");
		Assert.notNull(sql, "An SQL statement is required.");
		List<String> namedParameters = new ArrayList<>();
		parameterCount = JdbcParameterUtils.countParameterPlaceholders(sql, namedParameters);
		if (namedParameters.size() > 0) {
			if (parameterCount != namedParameters.size()) {
				throw new InvalidDataAccessApiUsageException("You can't use both named parameters and classic \"?\" placeholders: " + sql);
			}
			usingNamedParameters = true;
		}
		if (!usingNamedParameters) {
			Assert.notNull(itemPreparedStatementSetter, "Using SQL statement with '?' placeholders requires an ItemPreparedStatementSetter");
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.batch.item.ItemWriter#write(java.util.List)
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	@Override
	public void write(final List<? extends T> items) throws Exception {

		if (!items.isEmpty()) {

			if (logger.isDebugEnabled()) {
				logger.debug("Executing batch with " + items.size() + " items.");
			}

			int[] updateCounts;

			if (usingNamedParameters) {
				if(items.get(0) instanceof Map && this.itemSqlParameterSourceProvider == null) {
					updateCounts = namedParameterJdbcTemplate.batchUpdate(sql, items.toArray(new Map[items.size()]));
				} else {
					SqlParameterSource[] batchArgs = new SqlParameterSource[1]; //items.size()
					int i = 0;
					for (T item : items) {
						EricThread tmp = (EricThread) item;
						if(tmp.getCol1().substring(0, 1).equals("0"))
						{
							sql ="INSERT INTO ERIC_THREAD (COL1, COL2,COL3,COL4,COL5,COL6,COL7) VALUES (:col1,:col2,:col3,:col4,:col5,:col6,:col7)";
						}else {
							sql = "INSERT INTO ERIC_THREAD2 (COL1, COL2,COL3,COL4,COL5,COL6,COL7) VALUES (:col1,:col2,:col3,:col4,:col5,:col6,:col7)";
						}
						System.out.println("=====ExecuteSQL==="+sql);
						//batchArgs[i++] = itemSqlParameterSourceProvider.createSqlParameterSource(item);
						batchArgs[0] = itemSqlParameterSourceProvider.createSqlParameterSource(item);
						updateCounts = namedParameterJdbcTemplate.batchUpdate(sql, batchArgs);
					}
					//updateCounts = namedParameterJdbcTemplate.batchUpdate(sql, batchArgs);
				}
			}
			else {
			
				updateCounts = namedParameterJdbcTemplate.getJdbcOperations().execute(sql, new PreparedStatementCallback<int[]>() {
					@Override
					public int[] doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
						for (T item : items) {
							itemPreparedStatementSetter.setValues(item, ps);
							ps.addBatch();
						}
						return ps.executeBatch();
					}
				});
			}

			//if (assertUpdates) {
			//	for (int i = 0; i < updateCounts.length; i++) {
			//		int value = updateCounts[i];
			//		if (value == 0) {
			//			throw new EmptyResultDataAccessException("Item " + i + " of " + updateCounts.length
			//					+ " did not update any rows: [" + items.get(i) + "]", 1);
			//		}
			//	}
			//}
		}
	}
}
