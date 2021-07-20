package cn.tenmg.sql.paging.dialect;

import java.sql.Connection;
import java.util.Map;

import cn.tenmg.sql.paging.SQLMetaData;

/**
 * PostgreSQL方言
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 *
 * @since 1.0.0
 */
public class PostgreSQLPagingDialect extends AbstractSQLPagingDialect {

	private static final String PAGE_WRAP_START = "SELECT * FROM (\n", PAGE_WRAP_END = "\n) SQLTOOL",
			LIMIT = " LIMIT %d OFFSET %d";

	private static final PostgreSQLPagingDialect INSTANCE = new PostgreSQLPagingDialect();

	public static final PostgreSQLPagingDialect getInstance() {
		return INSTANCE;
	}

	protected PostgreSQLPagingDialect() {
		super();
	}

	@Override
	public String pageSql(Connection con, String sql, Map<String, ?> params, SQLMetaData sqlMetaData, int pageSize,
			long currentPage) {
		int selectIndex = sqlMetaData.getSelectIndex();
		if (selectIndex < 0) {// 正常情况下selectIndex不可能<0，但如果用户的确写错了，这里直接返回错误的SQL
			return sql;
		} else {
			int length = sqlMetaData.getLength(), embedEndIndex = sqlMetaData.getEmbedEndIndex();
			if (sqlMetaData.getLimitIndex() >= 0) {
				int embedStartIndex = sqlMetaData.getEmbedStartIndex();
				if (embedStartIndex > 0) {
					if (embedEndIndex < length) {
						return sql.substring(0, embedStartIndex).concat(PAGE_WRAP_START)
								.concat(sql.substring(embedStartIndex, embedEndIndex))
								.concat(pageEnd(pageSize, currentPage)).concat(sql.substring(embedEndIndex));
					} else {
						return sql.substring(0, embedStartIndex).concat(PAGE_WRAP_START)
								.concat(sql.substring(embedStartIndex)).concat(pageEnd(pageSize, currentPage));
					}
				} else {
					if (embedEndIndex < length) {
						return PAGE_WRAP_START.concat(sql.substring(0, embedEndIndex))
								.concat(pageEnd(pageSize, currentPage)).concat(sql.substring(embedEndIndex));
					} else {
						return PAGE_WRAP_START.concat(sql).concat(pageEnd(pageSize, currentPage));
					}
				}
			} else {
				if (embedEndIndex < length) {
					return sql.substring(0, embedEndIndex).concat(generateLimit(pageSize, currentPage))
							.concat(sql.substring(embedEndIndex));
				} else {
					return sql.concat(generateLimit(pageSize, currentPage));
				}
			}
		}
	}

	private static String pageEnd(int pageSize, long currentPage) {
		return PAGE_WRAP_END.concat(generateLimit(pageSize, currentPage));
	}

	private static String generateLimit(int pageSize, long currentPage) {
		return String.format(LIMIT, pageSize, (currentPage - 1) * pageSize);
	}

}
