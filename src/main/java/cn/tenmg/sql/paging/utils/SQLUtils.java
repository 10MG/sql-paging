package cn.tenmg.sql.paging.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
//import java.util.Set;

import cn.tenmg.dsl.Script;
import cn.tenmg.dsl.parser.JDBCParamsParser;
import cn.tenmg.dsl.utils.DSLUtils;
//import cn.tenmg.dsl.utils.SetUtils;
import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.sql.paging.SQLMetaData;

/**
 * SQL工具类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.0.0
 *
 */
public abstract class SQLUtils {

	public static final char BACKSLASH = '\\', BLANK_SPACE = '\u0020', LEFT_BRACKET = '\u0028',
			RIGHT_BRACKET = '\u0029', COMMA = ',', SINGLE_QUOTATION_MARK = '\'', PARAM_MARK = '?',
			LINE_SEPARATOR[] = { '\r', '\n' };

	private static final String WITH = "WITH", SELECT = "SELECT", FROM = "FROM", WHERE = "WHERE", GROUP = "GROUP",
			BY = "BY", HAVING = "HAVING", ORDER = "ORDER", LIMIT = "LIMIT", OFFSET = "OFFSET", FETCH = "FETCH",
			UNION = "UNION", AND = "AND", BLANK_SPACE_AND = " " + AND, IMPOSSIBLE = " 1=0",
			WHERE_IMPOSSIBLE = "WHERE" + IMPOSSIBLE, BLANK_SPACE_WHERE_IMPOSSIBLE = BLANK_SPACE + WHERE_IMPOSSIBLE;

	private static final int WITH_LEN = WITH.length(), SELECT_LEN = SELECT.length(), FROM_LEN = FROM.length(),
			WHERE_LEN = WHERE.length(), GROUP_LEN = GROUP.length(), HAVING_LEN = HAVING.length(),
			ORDER_LEN = ORDER.length(), LIMIT_LEN = LIMIT.length(), OFFSET_LEN = OFFSET.length(),
			FETCH_LEN = FETCH.length();

	/**
	 * 获取SQL相关数据（不对SQL做 {@code null} 校验）
	 * 
	 * @param sql
	 *            SQL
	 * @return 返回SQL相关数据对象
	 */
	public static SQLMetaData getSQLMetaData(String sql) {
		SQLMetaData sqlMetaData = new SQLMetaData();
		int i = 0, deep = 0, len = sql.length();
		sqlMetaData.setLength(len);
		int backslashes = 0, groupOrOrderIndex = -1;
		char a = BLANK_SPACE, b = BLANK_SPACE;
		boolean isWith = false, // 是否在WITH子句区域
				isString = false, // 是否在字符串区域
				isSinglelineComment = false, // 是否在单行注释区域
				isMiltilineComment = false; // 是否在WITH子句区域
		StringBuilder wordBefore = new StringBuilder(), currentWord = new StringBuilder();
		while (i < len) {
			char c = sql.charAt(i);
			if (isWith) {
				if (isString) {
					if (c == BACKSLASH) {
						backslashes++;
					} else {
						if (DSLUtils.isStringEnd(a, b, c, backslashes)) {// 字符串区域结束
							isString = false;
						}
					}
				} else {
					if (c == SINGLE_QUOTATION_MARK) {// 字符串区域开始
						isString = true;
						wordBefore.setLength(0);
						currentWord.setLength(0);
					} else {
						if (c == LEFT_BRACKET) {// 左括号
							deep++;
							currentWord.setLength(0);
						} else if (c == RIGHT_BRACKET) {// 右括号
							deep--;
							currentWord.setLength(0);
						} else if (c <= BLANK_SPACE) {// 遇到空白字符
							if (deep == 0) {
								if (SELECT.equalsIgnoreCase(currentWord.toString())) {
									sqlMetaData.setSelectIndex(i - SELECT_LEN);
									isWith = false;
								}
							}
							currentWord.setLength(0);
						} else {
							currentWord.append(c);// 拼接单词
						}
					}
				}
			} else if (isString) {
				if (c == BACKSLASH) {
					backslashes++;
				} else {
					if (DSLUtils.isStringEnd(a, b, c, backslashes)) {// 字符串区域结束
						isString = false;
					}
					backslashes = 0;
				}
			} else if (c == SINGLE_QUOTATION_MARK) {// 字符串区域开始
				wordBefore.setLength(0);
				currentWord.setLength(0);
				isString = true;
			} else if (isSinglelineComment) {// 单行注释内
				if (c == DSLUtils.LINE_BREAK) {
					isSinglelineComment = false;
				}
			} else if (isMiltilineComment) {// 多行注释内
				if (DSLUtils.isMiltilineCommentEnd(b, c)) {
					isMiltilineComment = false;
				}
			} else if (DSLUtils.isSinglelineCommentBegin(b, c)) {// 单行注释开始
				wordBefore.setLength(0);
				currentWord.setLength(0);
				isSinglelineComment = true;
			} else if (DSLUtils.isMiltilineCommentBegin(b, c)) {// 多行注释开始
				wordBefore.setLength(0);
				currentWord.setLength(0);
				isMiltilineComment = true;
			} else {
				if (c == LEFT_BRACKET) {// 左括号
					deep++;
					wordBefore.setLength(0);
					currentWord.setLength(0);
				} else if (c == RIGHT_BRACKET) {// 右括号
					deep--;
					wordBefore.setLength(0);
					currentWord.setLength(0);
				} else if (c <= BLANK_SPACE) {// 遇到空白字符
					if (deep == 0) {
						String cw = currentWord.toString();
						if (WITH.equalsIgnoreCase(cw)) {
							sqlMetaData.setWithIndex(i - WITH_LEN);
							isWith = true;
						} else if (SELECT.equalsIgnoreCase(cw)) {
							if (sqlMetaData.getSelectIndex() < 0) {
								sqlMetaData.setSelectIndex(i - SELECT_LEN);
							}
						} else if (FROM.equalsIgnoreCase(cw)) {
							if (sqlMetaData.getFromIndex() < 0) {
								sqlMetaData.setFromIndex(i - FROM_LEN);
							}
						} else if (WHERE.equalsIgnoreCase(cw)) {
							sqlMetaData.setWhereIndex(i - WHERE_LEN);
						} else if (GROUP.equalsIgnoreCase(cw)) {
							groupOrOrderIndex = i - GROUP_LEN;
						} else if (BY.equalsIgnoreCase(cw)) {
							String wb = wordBefore.toString();
							if (GROUP.equalsIgnoreCase(wb)) {
								sqlMetaData.setGroupByIndex(groupOrOrderIndex);
							} else if (ORDER.equalsIgnoreCase(wb)) {
								sqlMetaData.setOrderByIndex(groupOrOrderIndex);
							}
						} else if (ORDER.equalsIgnoreCase(cw)) {
							groupOrOrderIndex = i - ORDER_LEN;
						} else if (HAVING.equalsIgnoreCase(cw)) {
							sqlMetaData.setHavingIndex(i - HAVING_LEN);
						} else if (LIMIT.equalsIgnoreCase(cw)) {
							sqlMetaData.setLimitIndex(i - LIMIT_LEN);
						} else if (OFFSET.equalsIgnoreCase(cw)) {
							sqlMetaData.setOffsetIndex(i - OFFSET_LEN);
						} else if (FETCH.equalsIgnoreCase(cw)) {
							sqlMetaData.setFetchIndex(i - FETCH_LEN);
						} else if (UNION.equalsIgnoreCase(cw)) {
							sqlMetaData.setUnion(true);
						}
						wordBefore.setLength(0);
						wordBefore.append(currentWord);
					}
					currentWord.setLength(0);
				} else {
					currentWord.append(c);// 拼接单词
				}
			}
			a = b;
			b = c;
			i++;
		}
		return sqlMetaData;
	}

	/**
	 * 获取SQL字段名列表
	 * 
	 * @param con
	 *            已打开的数据库连接
	 * @param namedSQL
	 *            命名参数SQL
	 * @param params
	 *            查询参数集
	 * @param sqlMetaData
	 *            SQL相关数据对象
	 * @return 返回SQL字段名列表
	 * @throws SQLException
	 *             SQL异常
	 */
	public static final String[] getColumnLabels(Connection con, String namedSQL, Map<String, ?> params,
			SQLMetaData sqlMetaData) throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		String columnLabels[] = null;
		try {
			int whereIndex = sqlMetaData.getWhereIndex(), limitIndex = sqlMetaData.getLimitIndex(),
					firstStatmentIndexAfterWhere = firstStatmentIndexAfterWhere(sqlMetaData);
			if (limitIndex < 0) {
				limitIndex = sqlMetaData.getOffsetIndex();
			}
			if (limitIndex < 0) {
				limitIndex = sqlMetaData.getFetchIndex();
			}
			if (whereIndex > 0) {
				if (firstStatmentIndexAfterWhere > 0) {
					if (limitIndex > firstStatmentIndexAfterWhere) {// 含LIMIT/OFFSET/FETCH子句
						namedSQL = StringUtils.concat(namedSQL.substring(0, firstStatmentIndexAfterWhere), AND,
								IMPOSSIBLE, namedSQL.substring(firstStatmentIndexAfterWhere, limitIndex));
					} else {// 不含LIMIT/OFFSET/FETCH子句
						namedSQL = StringUtils.concat(namedSQL.substring(0, firstStatmentIndexAfterWhere), AND,
								IMPOSSIBLE, namedSQL.substring(firstStatmentIndexAfterWhere));
					}
				} else {
					if (limitIndex > firstStatmentIndexAfterWhere) {// 含LIMIT/OFFSET/FETCH子句
						namedSQL = StringUtils.concat(namedSQL.substring(0, limitIndex), BLANK_SPACE_AND, IMPOSSIBLE);
					} else {// 不含LIMIT/OFFSET/FETCH子句
						namedSQL = StringUtils.concat(namedSQL, BLANK_SPACE_AND, IMPOSSIBLE);
					}
				}
			} else {
				if (firstStatmentIndexAfterWhere > 0) {
					if (limitIndex > firstStatmentIndexAfterWhere) {// 含LIMIT/OFFSET/FETCH子句
						namedSQL = StringUtils.concat(namedSQL.substring(0, firstStatmentIndexAfterWhere),
								WHERE_IMPOSSIBLE, namedSQL.substring(firstStatmentIndexAfterWhere, limitIndex));
					} else {// 不含LIMIT/OFFSET/FETCH子句
						namedSQL = StringUtils.concat(namedSQL.substring(0, firstStatmentIndexAfterWhere),
								WHERE_IMPOSSIBLE, namedSQL.substring(firstStatmentIndexAfterWhere));
					}
				} else {
					if (limitIndex > firstStatmentIndexAfterWhere) {// 含LIMIT/OFFSET/FETCH子句
						namedSQL = StringUtils.concat(namedSQL.substring(0, limitIndex), BLANK_SPACE_WHERE_IMPOSSIBLE);
					} else {// 不含LIMIT/OFFSET/FETCH子句
						namedSQL = StringUtils.concat(namedSQL, BLANK_SPACE_WHERE_IMPOSSIBLE);
					}
				}
			}
			Script<List<Object>> sql = DSLUtils.toScript(namedSQL, params, JDBCParamsParser.getInstance());
			ps = con.prepareStatement(sql.getValue());
			JDBCUtils.setParams(ps, sql.getParams());
			rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			columnLabels = new String[columnCount];
			for (int i = 1; i <= columnCount; i++) {
				columnLabels[i - 1] = rsmd.getColumnLabel(i);
			}
		} finally {
			JDBCUtils.close(rs);
			JDBCUtils.close(ps);
		}
		return columnLabels;
	}

	private static int firstStatmentIndexAfterWhere(SQLMetaData sqlMetaData) {
		int index = sqlMetaData.getGroupByIndex();
		if (index < 0) {
			index = sqlMetaData.getHavingIndex();
		}
		if (index < 0) {
			index = sqlMetaData.getOrderByIndex();
		}
		return index;
	}

}
