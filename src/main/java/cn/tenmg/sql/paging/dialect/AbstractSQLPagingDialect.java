package cn.tenmg.sql.paging.dialect;

import java.util.Properties;

import cn.tenmg.dsl.utils.PropertiesLoaderUtils;
import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.sql.paging.SQLMetaData;
import cn.tenmg.sql.paging.SQLPagingDialect;

/**
 * 抽象SQL分页查询方言。封装方言基本方法
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.0.0
 */
public abstract class AbstractSQLPagingDialect implements SQLPagingDialect {

	protected static final String SELECT = "SELECT", COUNT,
			COUNT_REGEX = "[\\s]*[C|c][O|o][U|u][N|n][T|t]\\([\\S]+\\)[\\s]*", COUNT_START, COUNT_END = ") SQL_PAGING";

	protected static final int SELECT_LEN = SELECT.length();

	protected static final Properties config = new Properties();

	static {
		PropertiesLoaderUtils.loadIgnoreException(config, "sql-paging.properties");
		COUNT = StringUtils.concat(" COUNT(", config.getProperty("count.expression", "*"), ") ");
		COUNT_START = StringUtils.concat(SELECT, COUNT, "FROM (");
	}

	@Override
	public String countSql(String namedSql, SQLMetaData sqlMetaData) {
		boolean isUnion = sqlMetaData.isUnion();
		int selectIndex = sqlMetaData.getSelectIndex(), orderByIndex = sqlMetaData.getOrderByIndex(),
				limitIndex = sqlMetaData.getLimitIndex();
		if (limitIndex > 0) {// 含有LIMIT子句
			return wrapLimitedSqlToCountSql(namedSql, selectIndex, orderByIndex, limitIndex, isUnion);
		} else {
			int offsetIndex = sqlMetaData.getOffsetIndex();
			if (offsetIndex > 0) {// 含有OFFSET子句
				return wrapLimitedSqlToCountSql(namedSql, selectIndex, orderByIndex, offsetIndex, isUnion);
			} else {
				int fetchIndex = sqlMetaData.getFetchIndex();
				if (fetchIndex > 0) {// 含有OFFSET子句
					return wrapLimitedSqlToCountSql(namedSql, selectIndex, orderByIndex, fetchIndex, isUnion);
				} else {
					return wrapUnLimitedSqlToCountSql(namedSql, selectIndex, sqlMetaData.getFromIndex(),
							sqlMetaData.getGroupByIndex(), orderByIndex, isUnion);
				}
			}
		}
	}

	/**
	 * 包装不含行数限定条件的查询SQL为查询总记录数的SQL
	 * 
	 * @param namedSql
	 *            可能含命名参数的查询SQL
	 * @param selectIndex
	 *            SELECT子句的开始位置
	 * @param fromIndex
	 *            FROM子句的开始位置
	 * @param groupByIndex
	 *            GROUP BY子句的开始位置
	 * @param orderByIndex
	 *            ORDER BY子句的开始位置
	 * @param isUnion
	 *            主查询是否含有 UNION 子句
	 * @return 查询总记录数的SQL
	 */
	protected static String wrapUnLimitedSqlToCountSql(String namedSql, int selectIndex, int fromIndex,
			int groupByIndex, int orderByIndex, boolean isUnion) {
		if (selectIndex >= 0) {
			if (isUnion) {
				if (orderByIndex > selectIndex) {
					return StringUtils.concat(namedSql.substring(0, selectIndex), COUNT_START,
							namedSql.substring(selectIndex, orderByIndex), COUNT_END);
				} else {
					return StringUtils.concat(namedSql.substring(0, selectIndex), COUNT_START,
							namedSql.substring(selectIndex), COUNT_END);
				}
			} else {
				if (fromIndex > selectIndex && groupByIndex < fromIndex) {
					int columnsBegin = selectIndex + SELECT_LEN;
					if (namedSql.substring(columnsBegin, fromIndex).matches(COUNT_REGEX)) {
						return namedSql;
					} else {
						String select = namedSql.substring(0, columnsBegin);
						if (orderByIndex > fromIndex) {
							return StringUtils.concat(select, COUNT, namedSql.substring(fromIndex, orderByIndex));
						} else {
							return StringUtils.concat(select, COUNT, namedSql.substring(fromIndex));
						}
					}
				} else {
					if (orderByIndex > fromIndex) {
						return StringUtils.concat(namedSql.substring(0, selectIndex), COUNT_START,
								namedSql.substring(selectIndex, orderByIndex), COUNT_END);
					} else {
						return StringUtils.concat(namedSql.substring(0, selectIndex), COUNT_START,
								namedSql.substring(selectIndex), COUNT_END);
					}
				}
			}
		} else {
			return StringUtils.concat(COUNT_START, namedSql, COUNT_END);
		}
	}

	/**
	 * 包装含有行数限定条件的查询SQL为查询总记录数的SQL
	 * 
	 * @param namedSql
	 *            可能含命名参数的查询SQL
	 * @param selectIndex
	 *            SELECT子句的开始位置
	 * @param orderByIndex
	 *            ORDER BY子句的开始位置
	 * @param firstStatementIndexAfterOrderby
	 *            ORDER BY下一子句的开始位置
	 * @param isUnion
	 *            主查询是否含有 UNION 子句
	 * @return 查询总记录数的SQL
	 */
	private static String wrapLimitedSqlToCountSql(String namedSql, int selectIndex, int orderByIndex,
			int firstStatementIndexAfterOrderby, boolean isUnion) {// 有 LIMIT、OFFSET或者FETCH子句
		if (selectIndex >= 0) {
			if (orderByIndex > selectIndex) {// 含有 ORDER BY 子句
				if (firstStatementIndexAfterOrderby > orderByIndex) {
					namedSql = StringUtils.concat(namedSql.substring(0, selectIndex), COUNT_START,
							namedSql.substring(selectIndex, orderByIndex),
							namedSql.substring(firstStatementIndexAfterOrderby), COUNT_END);
				} else if (isUnion) {// 含有 UNION 子句
					namedSql = StringUtils.concat(namedSql.substring(0, selectIndex), COUNT_START,
							namedSql.substring(selectIndex, orderByIndex), COUNT_END);
				} else {
					namedSql = StringUtils.concat(namedSql.substring(0, selectIndex), COUNT_START,
							namedSql.substring(selectIndex), COUNT_END);
				}
			} else if (isUnion) {// 含有 UNION 子句
				namedSql = StringUtils.concat(namedSql.substring(0, selectIndex), COUNT_START,
						namedSql.substring(selectIndex), COUNT_END);
			} else {
				namedSql = StringUtils.concat(namedSql.substring(0, selectIndex), COUNT_START,
						namedSql.substring(selectIndex), COUNT_END);
			}
		} else {
			namedSql = StringUtils.concat(COUNT_START, namedSql, COUNT_END);
		}
		return namedSql;
	}

}
