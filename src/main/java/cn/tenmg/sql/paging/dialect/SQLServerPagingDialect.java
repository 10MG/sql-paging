package cn.tenmg.sql.paging.dialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import cn.tenmg.dsl.utils.DSLUtils;
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

	private static final String CONST_RN = "1 RN__,", PAGE_WRAP_START = "SELECT %s FROM (",
			SUBQUERY_START = "SELECT " + CONST_RN + "SQL_PAGING.* FROM (", SUBQUERY_END = ") SQL_PAGING",
			ORDER_BY = "ORDER BY RN__", PAGE_WRAP_END = " OFFSET %d ROW FETCH NEXT %d ROW ONLY";

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
			if (offsetIndex > selectIndex) {// 有OFFSET子句，直接包装子查询
				return wrapCountSql(namedSql, selectIndex, sqlMetaData.getOrderByIndex(), offsetIndex,
						sqlMetaData.isUnion());
			} else {// 没有OFFSET子句
				int fetchIndex = sqlMetaData.getFetchIndex();
				if (offsetIndex > selectIndex) {// 有FETCH子句，按理说SQLServer含FETCH必须含OFFSET（这里假设以后SQLServer支持没有OFFSET的FETCH）
					return wrapCountSql(namedSql, selectIndex, sqlMetaData.getOrderByIndex(), fetchIndex,
							sqlMetaData.isUnion());
				} else {
					if (sqlMetaData.isUnion()) {// 含有UNION子句
						int orderByIndex = sqlMetaData.getOrderByIndex();
						if (orderByIndex > selectIndex) {// 有 ORDER BY 子句
							return StringUtils.concat(namedSql.substring(0, selectIndex), COUNT_START,
									namedSql.substring(selectIndex, orderByIndex), COUNT_END);
						} else {// 没有 ORDER BY 子句
							return StringUtils.concat(namedSql.substring(0, selectIndex), COUNT_START,
									namedSql.substring(selectIndex), COUNT_END);
						}
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
									return StringUtils.concat(select, COUNT,
											namedSql.substring(fromIndex, orderByIndex));
								} else {
									return StringUtils.concat(select, COUNT, namedSql.substring(fromIndex));
								}
							}
						} else {
							return StringUtils.concat(COUNT_START, namedSql, COUNT_END);
						}
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
						pageEnd, selectIndex, sqlMetaData.getOrderByIndex(), offsetIndex, sqlMetaData.isUnion());
			} else {// 没有OFFSET子句
				int fetchIndex = sqlMetaData.getFetchIndex();
				if (fetchIndex > selectIndex) {// 有FETCH子句，按理说SQLServer含FETCH必须含OFFSET（这里假设以后SQLServer支持没有OFFSET的FETCH）
					return wrapPageSql(namedSql,
							pageStart(SQLUtils.getColumnLabels(con, namedSql, params, sqlMetaData)), pageEnd,
							selectIndex, sqlMetaData.getOrderByIndex(), fetchIndex, sqlMetaData.isUnion());
				} else {
					int orderByIndex = sqlMetaData.getOrderByIndex();
					if (orderByIndex > 0) {// 没有OFFSET子句但有ORDER BY子句，直接在末尾追加行数限制条件
						return namedSql.concat(pageEnd);
					} else {// 没有OFFSET子句也没有ORDER BY子句，增加一常量列并按此列排序，再追加行数限制条件
						int selectEndIndex = selectIndex + SELECT_LEN;
						return StringUtils.concat(namedSql.substring(0, selectIndex),
								pageStart(SQLUtils.getColumnLabels(con, namedSql, params, sqlMetaData)),
								namedSql.substring(selectIndex, selectEndIndex), SQLUtils.BLANK_SPACE, CONST_RN,
								insertRowNumAfterSelect(namedSql.substring(selectEndIndex)), SQLUtils.BLANK_SPACE,
								ORDER_BY, pageEnd, SUBQUERY_END);
					}
				}
			}
		} else {
			return StringUtils.concat(pageStart(SQLUtils.getColumnLabels(con, namedSql, params, sqlMetaData)),
					SUBQUERY_START, namedSql, SUBQUERY_END, SQLUtils.BLANK_SPACE, ORDER_BY, pageEnd, SUBQUERY_END);
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
	 * @param isUnion
	 *            主查询是否含有 UNION 子句
	 * @return 返回查询总记录数的SQL
	 */
	private static String wrapCountSql(String namedSql, int selectIndex, int orderByIndex,
			int firstStatementIndexAfterOrderby, boolean isUnion) {// 有 OFFSET 或者 FETCH 子句
		if (orderByIndex > selectIndex) {// 有 ORDER BY 子句
			namedSql = StringUtils.concat(namedSql.substring(0, selectIndex), COUNT_START,
					namedSql.substring(selectIndex), COUNT_END);
		} else {// 没有 ORDER BY 子句，则自动追加ORDER BY子句，避免报错
			int selectEndIndex = selectIndex + SELECT_LEN;
			if (isUnion) {// 有 UNION 子句
				namedSql = StringUtils.concat(namedSql.substring(0, selectIndex), COUNT_START,
						namedSql.substring(selectIndex, selectEndIndex), SQLUtils.BLANK_SPACE, CONST_RN,
						insertRowNumAfterSelect(namedSql.substring(selectEndIndex, firstStatementIndexAfterOrderby)),
						ORDER_BY, SQLUtils.BLANK_SPACE, namedSql.substring(firstStatementIndexAfterOrderby), COUNT_END);
			} else {
				namedSql = StringUtils.concat(namedSql.substring(0, selectIndex), COUNT_START,
						namedSql.substring(selectIndex, selectEndIndex), SQLUtils.BLANK_SPACE, CONST_RN,
						namedSql.substring(selectEndIndex, firstStatementIndexAfterOrderby), ORDER_BY,
						SQLUtils.BLANK_SPACE, namedSql.substring(firstStatementIndexAfterOrderby), COUNT_END);
			}
		}
		return namedSql;
	}

	private static StringBuilder insertRowNumAfterSelect(String sql) {
		int backslashes = 0, deep = 0;
		char a = SQLUtils.BLANK_SPACE, b = SQLUtils.BLANK_SPACE;
		boolean isString = false;// 是否在字符串区域
		StringBuilder sqlBuilder = new StringBuilder(), wordBuilder = new StringBuilder();
		for (int i = 0, len = sql.length(); i < len; i++) {
			char c = sql.charAt(i);
			if (isString) {
				if (c == SQLUtils.BACKSLASH) {
					backslashes++;
				} else {
					if (DSLUtils.isStringEnd(a, b, c, backslashes)) {// 字符串区域结束
						isString = false;
					}
					backslashes = 0;
				}
			} else {
				if (c == SQLUtils.SINGLE_QUOTATION_MARK) {// 字符串区域开始
					isString = true;
				} else {
					if (c == SQLUtils.LEFT_BRACKET) {// 左括号
						deep++;
						wordBuilder.setLength(0);
					} else if (c == SQLUtils.RIGHT_BRACKET) {// 右括号
						deep--;
						wordBuilder.setLength(0);
					} else if (c <= SQLUtils.BLANK_SPACE) {// 遇到空白字符
						if (deep == 0) {
							if (SELECT.equalsIgnoreCase(wordBuilder.toString())) {
								sqlBuilder.append(SQLUtils.BLANK_SPACE).append(CONST_RN);
							}
						}
						wordBuilder.setLength(0);
					} else {
						wordBuilder.append(c);// 拼接单词
					}
				}
			}
			sqlBuilder.append(c);
			a = b;
			b = c;
		}
		return sqlBuilder;
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
	 * @param isUnion
	 *            主查询是否含有 UNION 子句
	 * @return 返回查询总记录数的SQL
	 */
	private static String wrapPageSql(String namedSql, String pageStart, String pageEnd, int selectIndex,
			int orderByIndex, int firstStatementIndexAfterOrderby, boolean isUnion) {
		if (orderByIndex > selectIndex) {
			return StringUtils.concat(namedSql.substring(0, selectIndex), pageStart, SUBQUERY_START,
					namedSql.substring(selectIndex), SUBQUERY_END, SQLUtils.BLANK_SPACE, ORDER_BY, pageEnd,
					SUBQUERY_END);
		} else {
			int selectEndIndex = selectIndex + SELECT_LEN;
			if (isUnion) {// 有 UNION 子句
				namedSql = StringUtils.concat(namedSql.substring(0, selectIndex), pageStart,
						namedSql.substring(selectIndex, selectEndIndex), SQLUtils.BLANK_SPACE, CONST_RN,
						insertRowNumAfterSelect(namedSql.substring(selectEndIndex, firstStatementIndexAfterOrderby)),
						ORDER_BY, " ", namedSql.substring(firstStatementIndexAfterOrderby), SUBQUERY_END,
						SQLUtils.BLANK_SPACE, ORDER_BY, pageEnd);
			} else {
				namedSql = StringUtils.concat(namedSql.substring(0, selectIndex), pageStart,
						namedSql.substring(selectIndex, selectEndIndex), SQLUtils.BLANK_SPACE, CONST_RN,
						namedSql.substring(selectEndIndex, firstStatementIndexAfterOrderby), ORDER_BY, " ",
						namedSql.substring(firstStatementIndexAfterOrderby), SUBQUERY_END, SQLUtils.BLANK_SPACE,
						ORDER_BY, pageEnd);
			}
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
