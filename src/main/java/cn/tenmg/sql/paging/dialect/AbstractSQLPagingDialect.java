package cn.tenmg.sql.paging.dialect;

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

	protected static final String SELECT = "SELECT";

	protected static final int SELECT_LEN = SELECT.length();

	private static final String COUNT = " COUNT(*) ", COUNT_START = SELECT + " COUNT(*) FROM (\n",
			COUNT_END = "\n) SQLTOOL";

	@Override
	public String countSql(String namedSql, SQLMetaData sqlMetaData) {
		int embedStartIndex = sqlMetaData.getEmbedStartIndex(), embedEndIndex = sqlMetaData.getEmbedEndIndex(),
				length = sqlMetaData.getLength();
		if (sqlMetaData.getLimitIndex() > 0 || sqlMetaData.getOffsetIndex() > 0 || sqlMetaData.getFetchIndex() > 0
				|| sqlMetaData.getGroupByIndex() > 0) {// 含有LIMIT、OFFSET、FETCH或GROUP BY子句
			return wrapCountSql(namedSql, embedStartIndex, embedEndIndex, length);
		}
		int selectIndex = sqlMetaData.getSelectIndex(), fromIndex = sqlMetaData.getFromIndex(),
				orderByIndex = sqlMetaData.getOrderByIndex();
		if (selectIndex >= 0 && fromIndex > selectIndex) {// 正确拼写了SELECT、FROM子句、且不包含LIMIT、OFFSET、FETCH或GROUP BY子句
			if (orderByIndex > 0) {// 含ORDER BY子句
				if (selectIndex > 0) {
					return namedSql.substring(0, selectIndex)
							.concat(namedSql.substring(selectIndex, selectIndex + SELECT_LEN)).concat(COUNT)
							.concat(namedSql.substring(fromIndex, orderByIndex));
				} else {
					return namedSql.substring(selectIndex, selectIndex + SELECT_LEN).concat(COUNT)
							.concat(namedSql.substring(fromIndex, orderByIndex));
				}
			} else {
				return namedSql.substring(0, selectIndex)
						.concat(namedSql.substring(selectIndex, selectIndex + SELECT_LEN)).concat(COUNT)
						.concat(namedSql.substring(fromIndex));
			}
		}
		return wrapCountSql(namedSql, embedStartIndex, embedEndIndex, length);
	}

	/**
	 * 包装查询SQL为查询总记录数的SQL
	 * 
	 * @param namedSql
	 *            可能含命名参数的查询SQL
	 * @param embedStartIndex
	 *            可嵌套查询的开始位置
	 * @param embedEndIndex
	 *            可嵌套查询的结束位置
	 * @param length
	 *            SQL的长度
	 * @return 返回查询总记录数的SQL
	 */
	private static String wrapCountSql(String namedSql, int embedStartIndex, int embedEndIndex, int length) {
		if (embedStartIndex > 0) {
			if (embedEndIndex < length) {
				namedSql = namedSql.substring(0, embedStartIndex).concat(COUNT_START)
						.concat(namedSql.substring(embedStartIndex, embedEndIndex)).concat(COUNT_END)
						.concat(namedSql.substring(embedEndIndex));
			} else {
				namedSql = namedSql.substring(0, embedStartIndex).concat(COUNT_START)
						.concat(namedSql.substring(embedStartIndex)).concat(COUNT_END);
			}
		} else {
			if (embedEndIndex < length) {
				namedSql = COUNT_START.concat(namedSql.substring(0, embedEndIndex)).concat(COUNT_END)
						.concat(namedSql.substring(embedEndIndex));
			} else {
				namedSql = COUNT_START.concat(namedSql).concat(COUNT_END);
			}
		}
		return namedSql;
	}

}
