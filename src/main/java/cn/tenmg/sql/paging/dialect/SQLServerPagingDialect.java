package cn.tenmg.sql.paging.dialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

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

	private static final String SQLTOOL_RN = " 1 RN__,", PAGE_WRAP_START = "SELECT %s FROM (\n",
			SUBQUERY_START = "SELECT" + SQLTOOL_RN + "SQLTOOL.* FROM (\n", SUBQUERY_END = "\n) SQLTOOL",
			ORDER_BY = "\nORDER BY RN__", PAGE_WRAP_END = " OFFSET %d ROW FETCH NEXT %d ROW ONLY";

	private static final SQLServerPagingDialect INSTANCE = new SQLServerPagingDialect();

	public static final SQLServerPagingDialect getInstance() {
		return INSTANCE;
	}

	protected SQLServerPagingDialect() {
		super();
	}

	@Override
	public String pageSql(Connection con, String namedSql, Map<String, ?> params, SQLMetaData sqlMetaData, int pageSize,
			long currentPage) throws SQLException {
		int selectIndex = sqlMetaData.getSelectIndex();
		if (selectIndex < 0) {// 正常情况下selectIndex不可能<0，但如果用户的确写错了，这里直接返回错误的SQL
			return namedSql;
		} else {
			int offsetIndex = sqlMetaData.getOffsetIndex(), length = sqlMetaData.getLength(),
					embedStartIndex = sqlMetaData.getEmbedStartIndex(), embedEndIndex = sqlMetaData.getEmbedEndIndex();
			if (offsetIndex > 0) {// 有OFFSET子句，直接包装子查询并追加行数限制条件
				String pageStart = pageStart(SQLUtils.getColumnLabels(con, namedSql, params, sqlMetaData));
				if (embedStartIndex > 0) {
					if (embedEndIndex < length) {
						return namedSql.substring(0, embedStartIndex).concat(pageStart).concat(SUBQUERY_START)
								.concat(namedSql.substring(embedStartIndex, embedEndIndex)).concat(SUBQUERY_END)
								.concat(ORDER_BY).concat(pageEnd(pageSize, currentPage)).concat(SUBQUERY_END)
								.concat(namedSql.substring(embedEndIndex));
					} else {
						return namedSql.substring(0, embedStartIndex).concat(pageStart).concat(SUBQUERY_START)
								.concat(namedSql.substring(embedStartIndex)).concat(SUBQUERY_END).concat(ORDER_BY)
								.concat(pageEnd(pageSize, currentPage)).concat(SUBQUERY_END);
					}
				} else {
					if (embedEndIndex < length) {
						return pageStart.concat(SUBQUERY_START).concat(namedSql.substring(0, embedEndIndex))
								.concat(SUBQUERY_END).concat(ORDER_BY).concat(pageEnd(pageSize, currentPage))
								.concat(SUBQUERY_END).concat(namedSql.substring(embedEndIndex));
					} else {
						return pageStart.concat(SUBQUERY_START).concat(namedSql).concat(SUBQUERY_END).concat(ORDER_BY)
								.concat(pageEnd(pageSize, currentPage)).concat(SUBQUERY_END);
					}
				}
			} else {// 没有OFFSET子句
				int orderByIndex = sqlMetaData.getOrderByIndex();
				if (orderByIndex > 0) {// 没有OFFSET子句但有ORDER BY子句，直接在末尾追加行数限制条件
					if (embedEndIndex < length) {
						return namedSql.substring(0, embedEndIndex).concat(pageEnd(pageSize, currentPage))
								.concat(namedSql.substring(embedEndIndex));
					} else {
						return namedSql.concat(pageEnd(pageSize, currentPage));
					}
				} else {// 没有OFFSET子句也没有ORDER BY子句，增加一常量列并按此列排序，再追加行数限制条件
					String pageStart = pageStart(SQLUtils.getColumnLabels(con, namedSql, params, sqlMetaData));
					int selectEndIndex = selectIndex + SELECT_LEN;
					if (embedStartIndex > 0) {
						if (embedEndIndex < length) {
							return namedSql.substring(0, embedStartIndex).concat(pageStart)
									.concat(namedSql.substring(embedStartIndex, selectIndex))
									.concat(namedSql.substring(selectIndex, selectEndIndex)).concat(SQLTOOL_RN)
									.concat(namedSql.substring(selectEndIndex, embedEndIndex)).concat(ORDER_BY)
									.concat(pageEnd(pageSize, currentPage)).concat(SUBQUERY_END)
									.concat(namedSql.substring(embedEndIndex));
						} else {
							return namedSql.substring(0, embedStartIndex).concat(pageStart)
									.concat(namedSql.substring(embedStartIndex, selectIndex))
									.concat(namedSql.substring(selectIndex, selectEndIndex)).concat(SQLTOOL_RN)
									.concat(namedSql.substring(selectEndIndex)).concat(ORDER_BY)
									.concat(pageEnd(pageSize, currentPage)).concat(SUBQUERY_END);
						}
					} else {
						if (embedEndIndex < length) {
							return pageStart.concat(namedSql.substring(0, selectEndIndex)).concat(SQLTOOL_RN)
									.concat(namedSql.substring(selectEndIndex, embedEndIndex)).concat(ORDER_BY)
									.concat(pageEnd(pageSize, currentPage)).concat(SUBQUERY_END)
									.concat(namedSql.substring(embedEndIndex));
						} else {
							return pageStart.concat(namedSql.substring(0, selectEndIndex)).concat(SQLTOOL_RN)
									.concat(namedSql.substring(selectEndIndex)).concat(ORDER_BY)
									.concat(pageEnd(pageSize, currentPage)).concat(SUBQUERY_END);
						}
					}
				}
			}
		}
	}

	private static String pageStart(String[] columnLabels) {
		return String.format(PAGE_WRAP_START, String.join(", ", columnLabels));
	}

	private static String pageEnd(int pageSize, long currentPage) {
		return String.format(PAGE_WRAP_END, (currentPage - 1) * pageSize, pageSize);
	}

}
