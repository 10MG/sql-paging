package cn.tenmg.sql.paging.dialect;

import java.sql.Connection;
import java.util.Map;

import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.sql.paging.SQLMetaData;

/**
 * MySQL分页查询方言
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.0.0
 */
public class MySQLPagingDialect extends AbstractSQLPagingDialect {

	private static final String PAGE_WRAP_START = "SELECT * FROM (", PAGE_WRAP_END = ") SQL_PAGING",
			LIMIT = " LIMIT %d,%d";

	private static final MySQLPagingDialect INSTANCE = new MySQLPagingDialect();

	public static final MySQLPagingDialect getInstance() {
		return INSTANCE;
	}

	protected MySQLPagingDialect() {
		super();
	}

	@Override
	public String pageSql(Connection con, String namedSql, Map<String, ?> params, SQLMetaData sqlMetaData, int pageSize,
			long currentPage) {
		int selectIndex = sqlMetaData.getSelectIndex();
		if (selectIndex > 0) {
			if (sqlMetaData.getLimitIndex() >= 0) {
				return StringUtils.concat(namedSql.substring(0, selectIndex), PAGE_WRAP_START,
						namedSql.substring(selectIndex), PAGE_WRAP_END, generateLimit(pageSize, currentPage));
			} else {
				return namedSql.concat(generateLimit(pageSize, currentPage));
			}
		} else {
			return StringUtils.concat(PAGE_WRAP_START, namedSql, PAGE_WRAP_END, generateLimit(pageSize, currentPage));
		}
	}

	private static String generateLimit(int pageSize, long currentPage) {
		return String.format(LIMIT, (currentPage - 1) * pageSize, pageSize);
	}
}
