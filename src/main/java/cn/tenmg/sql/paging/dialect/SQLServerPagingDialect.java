package cn.tenmg.sql.paging.dialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.sql.paging.SQLMetaData;
import cn.tenmg.sql.paging.utils.SQLUtils;

/**
 * SQLServer分页查询方言
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.0.0
 */
public class SQLServerPagingDialect extends AbstractSQLPagingDialect {

	private static final String CONST_RN = " 1 RN__,", PAGE_WRAP_START = "SELECT %s FROM (",
			SUBQUERY_START = "SELECT" + CONST_RN + "SQL_PAGING.* FROM (", SUBQUERY_END = ") SQL_PAGING",
			ORDER_BY = " ORDER BY RN__", PAGE_WRAP_END = " OFFSET %d ROW FETCH NEXT %d ROW ONLY";

	private static final SQLServerPagingDialect INSTANCE = new SQLServerPagingDialect();

	public static SQLServerPagingDialect getInstance() {
		return INSTANCE;
	}

	protected SQLServerPagingDialect() {
		super();
	}

	@Override
	public String countSql(String namedSql, SQLMetaData sqlMetaData) {
		int selectIndex = sqlMetaData.getSelectIndex(), offsetIndex = sqlMetaData.getOffsetIndex();
		if (selectIndex >= 0) {
			if (offsetIndex > selectIndex) {
				return wrapCountSql(namedSql, selectIndex, sqlMetaData.getOrderByIndex(), offsetIndex);
			} else {
				int fetchIndex = sqlMetaData.getFetchIndex();
				if (offsetIndex > selectIndex) {
					return wrapCountSql(namedSql, selectIndex, sqlMetaData.getOrderByIndex(), fetchIndex);
				} else {
					int fromIndex = sqlMetaData.getFromIndex();
					if (fromIndex > selectIndex) {
						int columnsBegin = selectIndex + SELECT_LEN;
						if (namedSql.substring(columnsBegin, fromIndex).matches(COUNT_REGEX)) {
							return namedSql;
						} else {
							String select = namedSql.substring(0, columnsBegin);
							int orderByIndex = sqlMetaData.getOrderByIndex();
							if (orderByIndex > fromIndex) {
								return StringUtils.concat(select, COUNT, namedSql.substring(fromIndex, orderByIndex));
							} else {
								return StringUtils.concat(select, COUNT, namedSql.substring(fromIndex));
							}
						}
					} else {
						return StringUtils.concat(COUNT_START, namedSql, COUNT_END);
					}
				}
			}
		} else {
			return StringUtils.concat(COUNT_START, namedSql, COUNT_END);
		}
	}

	@Override
	public String pageSql(Connection con, String namedSql, Map<String, ?> params, SQLMetaData sqlMetaData, int pageSize,
			long currentPage) throws SQLException {
		int selectIndex = sqlMetaData.getSelectIndex();
		String pageEnd = pageEnd(pageSize, currentPage);
		if (selectIndex >= 0) {
			int offsetIndex = sqlMetaData.getOffsetIndex();
			if (offsetIndex > selectIndex) {// 有OFFSET子句，直接包装子查询并追加行数限制条件
				return wrapPageSql(namedSql, pageStart(SQLUtils.getColumnLabels(con, namedSql, params, sqlMetaData)),
						pageEnd, selectIndex, sqlMetaData.getOrderByIndex(), offsetIndex);
			} else {// 没有OFFSET子句
				int fetchIndex = sqlMetaData.getFetchIndex();
				if (fetchIndex > selectIndex) {// 有FETCH子句，按理说SQLServer含FETCH必须含OFFSET（这里假设以后SQLServer支持没有OFFSET的FETCH）
					return wrapPageSql(namedSql,
							pageStart(SQLUtils.getColumnLabels(con, namedSql, params, sqlMetaData)), pageEnd,
							selectIndex, sqlMetaData.getOrderByIndex(), fetchIndex);
				} else {
					int orderByIndex = sqlMetaData.getOrderByIndex();
					if (orderByIndex > 0) {// 没有OFFSET子句但有ORDER BY子句，直接在末尾追加行数限制条件
						return namedSql.concat(pageEnd);
					} else {// 没有OFFSET子句也没有ORDER BY子句，增加一常量列并按此列排序，再追加行数限制条件
						int selectEndIndex = selectIndex + SELECT_LEN;
						return StringUtils.concat(namedSql.substring(0, selectIndex),
								pageStart(SQLUtils.getColumnLabels(con, namedSql, params, sqlMetaData)),
								namedSql.substring(selectIndex, selectEndIndex), CONST_RN,
								namedSql.substring(selectEndIndex), ORDER_BY, pageEnd, SUBQUERY_END);
					}
				}
			}
		} else {
			return StringUtils.concat(pageStart(SQLUtils.getColumnLabels(con, namedSql, params, sqlMetaData)),
					SUBQUERY_START, namedSql, SUBQUERY_END, ORDER_BY, pageEnd, SUBQUERY_END);
		}

	}

	/**
	 * 包装查询SQL为查询总记录数的SQL
	 * 
	 * @param namedSql
	 *            可能含命名参数的查询SQL
	 * @param selectIndex
	 *            SELECT子句的开始位置
	 * @param orderByIndex
	 *            ORDER BY子句的开始位置
	 * @param firstStatementIndexAfterOrderby
	 *            ORDER BY下一子句的开始位置
	 * @return 返回查询总记录数的SQL
	 */
	private static String wrapCountSql(String namedSql, int selectIndex, int orderByIndex,
			int firstStatementIndexAfterOrderby) {
		if (orderByIndex > selectIndex || firstStatementIndexAfterOrderby < selectIndex) {
			namedSql = StringUtils.concat(namedSql.substring(0, selectIndex), COUNT_START,
					namedSql.substring(selectIndex), COUNT_END);
		} else {
			int selectEndIndex = selectIndex + SELECT_LEN;
			namedSql = StringUtils.concat(namedSql.substring(0, selectIndex), COUNT_START,
					namedSql.substring(selectIndex, selectEndIndex), CONST_RN,
					namedSql.substring(selectEndIndex, firstStatementIndexAfterOrderby), ORDER_BY, " ",
					namedSql.substring(firstStatementIndexAfterOrderby), COUNT_END);
		}
		return namedSql;
	}

	/**
	 * 包装查询SQL为查询总记录数的SQL
	 * 
	 * @param namedSql
	 *            可能含命名参数的查询SQL
	 * @param selectIndex
	 *            SELECT子句的开始位置
	 * @param orderByIndex
	 *            ORDER BY子句的开始位置
	 * @param firstStatementIndexAfterOrderby
	 *            ORDER BY下一子句的开始位置
	 * @return 返回查询总记录数的SQL
	 */
	private static String wrapPageSql(String namedSql, String pageStart, String pageEnd, int selectIndex,
			int orderByIndex, int firstStatementIndexAfterOrderby) {
		if (orderByIndex > selectIndex || firstStatementIndexAfterOrderby < selectIndex) {
			return StringUtils.concat(namedSql.substring(0, selectIndex), pageStart, SUBQUERY_START,
					namedSql.substring(selectIndex), SUBQUERY_END, ORDER_BY, pageEnd, SUBQUERY_END);
		} else {
			int selectEndIndex = selectIndex + SELECT_LEN;
			namedSql = StringUtils.concat(namedSql.substring(0, selectIndex), pageStart,
					namedSql.substring(selectIndex, selectEndIndex), CONST_RN,
					namedSql.substring(selectEndIndex, firstStatementIndexAfterOrderby), ORDER_BY, " ",
					namedSql.substring(firstStatementIndexAfterOrderby), SUBQUERY_END, ORDER_BY, pageEnd);
		}
		return namedSql;
	}

	private static String pageStart(String[] columnLabels) {
		return String.format(PAGE_WRAP_START, String.join(", ", columnLabels));
	}

	private static String pageEnd(int pageSize, long currentPage) {
		return String.format(PAGE_WRAP_END, (currentPage - 1) * pageSize, pageSize);
	}

}
