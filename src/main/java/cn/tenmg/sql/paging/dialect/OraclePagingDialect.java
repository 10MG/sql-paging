package cn.tenmg.sql.paging.dialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.sql.paging.SQLMetaData;
import cn.tenmg.sql.paging.utils.SQLUtils;

/**
 * Oracle分页查询方言
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.0.0
 */
public class OraclePagingDialect extends AbstractSQLPagingDialect {

	private static final String PAGE_WRAP_START = "SELECT %s FROM (SELECT ROWNUM RN__, SQL_PAGING.* FROM (",
			PAGE_WRAP_END = ") SQL_PAGING WHERE ROWNUM <= %d) WHERE RN__ > %d";

	private static final String SUBQUERY_START = "SELECT * FROM (", SUBQUERY_END = ") SQL_PAGING",
			NEW_PAGE_WRAP_END = " OFFSET %d ROW FETCH NEXT %d ROW ONLY";

	private static final OraclePagingDialect INSTANCE = new OraclePagingDialect();

	public static OraclePagingDialect getInstance() {
		return INSTANCE;
	}

	protected OraclePagingDialect() {
		super();
	}

	@Override
	public String pageSql(Connection con, String namedSql, Map<String, ?> params, SQLMetaData sqlMetaData, int pageSize,
			long currentPage) throws SQLException {
		int selectIndex = sqlMetaData.getSelectIndex();
		if (con == null || con.getMetaData().getDatabaseMajorVersion() < 12) {
			String pageStart = pageStart(SQLUtils.getColumnLabels(con, namedSql, params, sqlMetaData));
			if (selectIndex >= 0) {
				return StringUtils.concat(namedSql.substring(0, selectIndex), pageStart,
						namedSql.substring(selectIndex), pageEnd(pageSize, currentPage));
			} else {
				return StringUtils.concat(pageStart, namedSql, pageEnd(pageSize, currentPage));
			}
		} else {// 12c以上版本
			if (selectIndex >= 0) {
				if (sqlMetaData.getOffsetIndex() > 0 || sqlMetaData.getFetchIndex() > 0) {// 有OFFSET或FETCH子句，直接包装子查询并追加行数限制条件
					return StringUtils.concat(namedSql.substring(0, selectIndex), SUBQUERY_START,
							namedSql.substring(selectIndex), SUBQUERY_END, newPageEnd(pageSize, currentPage));
				} else {// 没有OFFSET或FETCH子句，直接在末尾追加行数限制条件
					return namedSql.concat(newPageEnd(pageSize, currentPage));
				}
			} else {
				return StringUtils.concat(SUBQUERY_START, namedSql, SUBQUERY_END, newPageEnd(pageSize, currentPage));
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
