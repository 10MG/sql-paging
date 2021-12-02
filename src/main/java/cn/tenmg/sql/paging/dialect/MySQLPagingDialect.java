package cn.tenmg.sql.paging.dialect;

import java.sql.Connection;
import java.util.Map;

import cn.tenmg.sql.paging.SQLMetaData;

/**
 * MySQL分页查询方言
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.0.0
 */
public class MySQLPagingDialect extends AbstractSQLPagingDialect {

	private static final String PAGE_WRAP_START = "SELECT * FROM (\n", PAGE_WRAP_END = "\n) SQLTOOL",
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
		if (selectIndex < 0) {// 正常情况下selectIndex不可能<0，但如果用户的确写错了，这里直接返回错误的SQL
			return namedSql;
		} else {
			int length = sqlMetaData.getLength(), embedEndIndex = sqlMetaData.getEmbedEndIndex();
			if (sqlMetaData.getLimitIndex() >= 0) {
				int embedStartIndex = sqlMetaData.getEmbedStartIndex();
				if (embedStartIndex > 0) {
					if (embedEndIndex < length) {
						return namedSql.substring(0, embedStartIndex).concat(PAGE_WRAP_START)
								.concat(namedSql.substring(embedStartIndex, embedEndIndex))
								.concat(pageEnd(pageSize, currentPage)).concat(namedSql.substring(embedEndIndex));
					} else {
						return namedSql.substring(0, embedStartIndex).concat(PAGE_WRAP_START)
								.concat(namedSql.substring(embedStartIndex)).concat(pageEnd(pageSize, currentPage));
					}
				} else {
					if (embedEndIndex < length) {
						return PAGE_WRAP_START.concat(namedSql.substring(0, embedEndIndex))
								.concat(pageEnd(pageSize, currentPage)).concat(namedSql.substring(embedEndIndex));
					} else {
						return PAGE_WRAP_START.concat(namedSql).concat(pageEnd(pageSize, currentPage));
					}
				}
			} else {
				if (embedEndIndex < length) {
					return namedSql.substring(0, embedEndIndex).concat(generateLimit(pageSize, currentPage))
							.concat(namedSql.substring(embedEndIndex));
				} else {
					return namedSql.concat(generateLimit(pageSize, currentPage));
				}
			}
		}
	}

	private static String pageEnd(int pageSize, long currentPage) {
		return PAGE_WRAP_END.concat(generateLimit(pageSize, currentPage));
	}

	private static String generateLimit(int pageSize, long currentPage) {
		return String.format(LIMIT, (currentPage - 1) * pageSize, pageSize);
	}
}
