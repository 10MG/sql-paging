package cn.tenmg.sql.paging.dialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import cn.tenmg.sql.paging.SQLMetaData;
import cn.tenmg.sql.paging.utils.SQLUtils;

/**
 * Oracle方言
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.0.0
 */
public class OraclePagingDialect extends AbstractSQLPagingDialect {

	private static final String PAGE_WRAP_START = "SELECT %s FROM (SELECT ROWNUM RN__, SQLTOOL.* FROM (\n",
			PAGE_WRAP_END = "\n) SQLTOOL WHERE ROWNUM <= %d) WHERE RN__ > %d";

	private static final String SUBQUERY_START = "SELECT * FROM (\n", SUBQUERY_END = "\n) SQLTOOL",
			NEW_PAGE_WRAP_END = " OFFSET %d ROW FETCH NEXT %d ROW ONLY";

	private static final OraclePagingDialect INSTANCE = new OraclePagingDialect();

	public static final OraclePagingDialect getInstance() {
		return INSTANCE;
	}

	protected OraclePagingDialect() {
		super();
	}

	@Override
	public String pageSql(Connection con, String namedSql, Map<String, ?> params, SQLMetaData sqlMetaData, int pageSize,
			long currentPage) throws SQLException {
		int selectIndex = sqlMetaData.getSelectIndex();
		if (selectIndex < 0) {// 正常情况下selectIndex不可能<0，但如果用户的确写错了，这里直接返回错误的SQL
			return namedSql;
		} else {
			if (con.getMetaData().getDatabaseMajorVersion() >= 12) {// 12c以上版本
				int length = sqlMetaData.getLength(), embedStartIndex = sqlMetaData.getEmbedStartIndex(),
						embedEndIndex = sqlMetaData.getEmbedEndIndex();
				if (sqlMetaData.getOffsetIndex() > 0 || sqlMetaData.getFetchIndex() > 0) {// 有OFFSET或FETCH子句，直接包装子查询并追加行数限制条件
					if (embedStartIndex > 0) {
						if (embedEndIndex < length) {
							return namedSql.substring(0, embedStartIndex).concat(SUBQUERY_START)
									.concat(namedSql.substring(embedStartIndex, embedEndIndex)).concat(SUBQUERY_END)
									.concat(newPageEnd(pageSize, currentPage))
									.concat(namedSql.substring(embedEndIndex));
						} else {
							return namedSql.substring(0, embedStartIndex).concat(SUBQUERY_START)
									.concat(namedSql.substring(embedStartIndex)).concat(SUBQUERY_END)
									.concat(newPageEnd(pageSize, currentPage));
						}
					} else {
						if (embedEndIndex < length) {
							return SUBQUERY_START.concat(namedSql.substring(0, embedEndIndex)).concat(SUBQUERY_END)
									.concat(newPageEnd(pageSize, currentPage))
									.concat(namedSql.substring(embedEndIndex));
						} else {
							return SUBQUERY_START.concat(namedSql).concat(SUBQUERY_END)
									.concat(newPageEnd(pageSize, currentPage));
						}
					}
				} else {// 没有OFFSET或FETCH子句，直接在末尾追加行数限制条件
					if (embedEndIndex < length) {
						return namedSql.substring(0, embedEndIndex).concat(newPageEnd(pageSize, currentPage))
								.concat(namedSql.substring(embedEndIndex));
					} else {
						return namedSql.concat(newPageEnd(pageSize, currentPage));
					}
				}
			} else {
				String pageStart = pageStart(SQLUtils.getColumnLabels(con, namedSql, params, sqlMetaData));
				int length = sqlMetaData.getLength(), embedStartIndex = sqlMetaData.getEmbedStartIndex(),
						embedEndIndex = sqlMetaData.getEmbedEndIndex();
				if (embedStartIndex > 0) {
					if (embedEndIndex < length) {
						return namedSql.substring(0, embedStartIndex).concat(pageStart)
								.concat(namedSql.substring(embedStartIndex, embedEndIndex))
								.concat(pageEnd(pageSize, currentPage)).concat(namedSql.substring(embedEndIndex));
					} else {
						return namedSql.substring(0, embedStartIndex).concat(pageStart)
								.concat(namedSql.substring(embedStartIndex)).concat(pageEnd(pageSize, currentPage));
					}
				} else {
					if (embedEndIndex < length) {
						return pageStart.concat(namedSql.substring(0, embedEndIndex))
								.concat(pageEnd(pageSize, currentPage)).concat(namedSql.substring(embedEndIndex));
					} else {
						return pageStart.concat(namedSql).concat(pageEnd(pageSize, currentPage));
					}
				}
			}
		}
	}

	private static String pageStart(String[] columnLabels) {
		return String.format(PAGE_WRAP_START, String.join(", ", columnLabels));
	}

	private static String pageEnd(int pageSize, long currentPage) {
		return String.format(PAGE_WRAP_END, currentPage * pageSize, (currentPage - 1) * pageSize);
	}

	private static String newPageEnd(int pageSize, long currentPage) {
		return String.format(NEW_PAGE_WRAP_END, (currentPage - 1) * pageSize, pageSize);
	}
}
