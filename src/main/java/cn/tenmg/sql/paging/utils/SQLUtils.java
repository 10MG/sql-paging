package cn.tenmg.sql.paging.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import cn.tenmg.dsl.Script;
import cn.tenmg.dsl.parser.JDBCParamsParser;
import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.sql.paging.SQLMetaData;

/**
 * SQL工具类
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.0.0
 *
 */
public abstract class SQLUtils {

	public static final char BACKSLASH = '\\', BLANK_SPACE = '\u0020', LEFT_BRACKET = '\u0028',
			RIGHT_BRACKET = '\u0029', COMMA = ',', SINGLE_QUOTATION_MARK = '\'', PARAM_MARK = '?',
			LINE_SEPARATOR[] = { '\r', '\n' };

	private static final String WITH = "WITH", SELECT = "SELECT", FROM = "FROM", FROM_REVERSE = "MORF",
			ON_REVERSE = "NO", WHERE_REVERSE = "EREHW", GROUP_REVERSE = "PUORG", ORDER_REVERSE = "REDRO",
			BY_REVERSE = "YB", LIMIT_REVERSE = "TIMIL", OFFSET_REVERSE = "TESFFO", FETCH_REVERSE = "HCTEF",
			SELECT_ALL = SELECT + " * FROM (\n", ALIAS = "\n) SQLTOOL", WHERE_IMPOSSIBLE = "\nWHERE 1=0";

	private static final int WITH_LEN = WITH.length(), SELECT_LEN = SELECT.length(), FROM_LEN = FROM.length();

	/**
	 * 获取SQL相关数据（不对SQL做null校验）
	 * 
	 * @param sql
	 *            SQL
	 * @return 返回SQL相关数据对象
	 */
	public static SQLMetaData getSQLMetaData(String sql) {
		SQLMetaData sqlMetaData = new SQLMetaData();
		sqlMetaData.setLength(sql.length());
		rightAnalysis(sql, sqlMetaData);
		leftAnalysis(sql, sqlMetaData);
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
		String script, columnLabels[] = null;
		try {
			int length = sqlMetaData.getLength(), embedStartIndex = sqlMetaData.getEmbedStartIndex(),
					embedEndIndex = sqlMetaData.getEmbedEndIndex();
			if (embedStartIndex > 0) {
				if (embedEndIndex < length) {
					script = namedSQL.substring(0, embedStartIndex).concat(SELECT_ALL)
							.concat(namedSQL.substring(embedStartIndex, embedEndIndex)).concat(ALIAS)
							.concat(WHERE_IMPOSSIBLE).concat(namedSQL.substring(embedEndIndex));
				} else {
					script = namedSQL.substring(0, embedStartIndex).concat(SELECT_ALL)
							.concat(namedSQL.substring(embedStartIndex)).concat(ALIAS).concat(WHERE_IMPOSSIBLE);
				}
			} else {
				if (embedEndIndex < length) {
					script = SELECT_ALL.concat(namedSQL.substring(0, embedEndIndex)).concat(ALIAS)
							.concat(WHERE_IMPOSSIBLE).concat(namedSQL.substring(embedEndIndex));
				} else {
					script = SELECT_ALL.concat(namedSQL).concat(ALIAS).concat(WHERE_IMPOSSIBLE);
				}
			}
			Script<List<Object>> SQL = DSLUtils.toScript(script, params, JDBCParamsParser.getInstance());
			ps = con.prepareStatement(SQL.getValue());
			JDBCUtils.setParams(ps, SQL.getParams());
			rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			columnLabels = new String[columnCount];
			for (int i = 1; i <= columnCount; i++) {
				columnLabels[i - 1] = rsmd.getColumnLabel(i);
			}
		} catch (SQLException e) {
			throw e;
		} finally {
			JDBCUtils.close(rs);
			JDBCUtils.close(ps);
		}
		return columnLabels;
	}

	/**
	 * 分析SQL右边部分
	 * 
	 * @param sql
	 *            SQL
	 * @param sqlMetaData
	 *            SQL相关数据对象
	 */
	private static void rightAnalysis(String sql, SQLMetaData sqlMetaData) {
		int length = sqlMetaData.getLength(), i = length - 1;
		char c = sql.charAt(i);
		boolean isString = false;
		int deep = 0, lineSplitorIndexs[] = { length, length };
		StringBuilder sba = new StringBuilder(), sbb = new StringBuilder();
		while (i > 0 && c <= BLANK_SPACE) {// 跳过空白字符
			decideLineSplitorIndex(lineSplitorIndexs, c, i);
			c = sql.charAt(--i);
		}
		setEmbedEndIndex(sqlMetaData, lineSplitorIndexs[0], lineSplitorIndexs[1]);
		while (i > 0) {
			if (isString) {
				if (i > 2) {
					char b = sql.charAt(--i);
					if (c == SINGLE_QUOTATION_MARK && b != BACKSLASH) {// 字符串区域结束
						isString = false;
					}
					c = b;
				} else {
					break;
				}
			} else {
				if (c == SINGLE_QUOTATION_MARK) {// 字符串区域开始（这里是倒序）
					isString = true;
					c = sql.charAt(--i);
				} else {
					if (c == RIGHT_BRACKET) {// 右括号
						deep++;
						sba.setLength(0);
						sba.setLength(0);
					} else if (c == LEFT_BRACKET) {// 左括号
						deep--;
						sba.setLength(0);
						sba.setLength(0);
					} else if (deep == 0) {// 深度为0，表示主查询
						if (c == COMMA) {// 逗号
							sba.setLength(0);
							sba.setLength(0);
						} else if (c <= BLANK_SPACE) {// 遇到空白字符
							String sa = sba.toString(), sb = sbb.toString();
							if (BY_REVERSE.equalsIgnoreCase(sa)) {
								if (GROUP_REVERSE.equalsIgnoreCase(sb)) {
									sqlMetaData.setGroupByIndex(i + 1);
									break;
								} else if (ORDER_REVERSE.equalsIgnoreCase(sb)) {
									sqlMetaData.setOrderByIndex(i + 1);
								}
							} else if (LIMIT_REVERSE.equalsIgnoreCase(sb)) {
								sqlMetaData.setLimitIndex(i + 1);
							} else if (FETCH_REVERSE.equalsIgnoreCase(sb)) {
								sqlMetaData.setFetchIndex(i + 1);
							} else if (OFFSET_REVERSE.equalsIgnoreCase(sb)) {
								sqlMetaData.setOffsetIndex(i + 1);
							} else if (WHERE_REVERSE.equalsIgnoreCase(sb)) {
								sqlMetaData.setWhereIndex(i + 1);
								break;
							} else if (ON_REVERSE.equalsIgnoreCase(sb)) {
								break;
							} else if (FROM_REVERSE.equalsIgnoreCase(sb)) {
								sqlMetaData.setFromIndex(i + 1);
								break;
							}
							sba = sbb;
							sbb = new StringBuilder();
						} else {
							sbb.append(c);// 拼接单词
						}
					}
					c = sql.charAt(--i);
				}
			}
		}
	}

	/**
	 * 分析SQL左边部分
	 * 
	 * @param sql
	 *            SQL
	 * @param sqlMetaData
	 *            SQL相关数据对象
	 */
	private static void leftAnalysis(String sql, SQLMetaData sqlMetaData) {
		int i = 0, deep = 0, max = sqlMetaData.getLength(), fromIndex = sqlMetaData.getFromIndex(),
				whereIndex = sqlMetaData.getWhereIndex();
		if (whereIndex > 0) {// 含有WHERE子句，只需扫描到WHERE之前即可
			max = whereIndex;
		} else if (fromIndex > 0) {// 没有WHERE子句，但有FROM子句，只需扫描到FROM之前即可
			max = fromIndex;
		}
		int backslashes = 0, lineSplitorIndexs[] = { 0, 0 };
		char[] charsBefore = { BLANK_SPACE, BLANK_SPACE };
		boolean isString = false, isWith = false;// 是否子字符串区域，是否在WITH子句区域
		StringBuilder sb = new StringBuilder();
		while (i < max) {
			char c = sql.charAt(i);
			if (isWith) {
				if (isString) {
					if (c == BACKSLASH) {
						backslashes++;
					} else {
						if (DSLUtils.isStringEnd(charsBefore[0], charsBefore[1], c, backslashes)) {// 字符串区域结束
							isString = false;
						}
					}
					i = stepForward(charsBefore, c, i);
				} else {
					if (c == SINGLE_QUOTATION_MARK) {// 字符串区域开始
						isString = true;
						i = stepForward(charsBefore, c, i);
					} else {
						if (c == LEFT_BRACKET) {// 左括号
							deep++;
							sb.setLength(0);
						} else if (c == RIGHT_BRACKET) {// 右括号
							deep--;
							sb.setLength(0);
						} else if (c <= BLANK_SPACE) {// 遇到空白字符
							if (deep == 0) {
								decideLineSplitorIndex(lineSplitorIndexs, c, i);
								String s = sb.toString();
								if (SELECT.equalsIgnoreCase(s)) {
									sqlMetaData.setSelectIndex(i - SELECT_LEN);
									isWith = false;
								}
							}
							sb.setLength(0);
						} else {
							sb.append(c);// 拼接单词
						}
						i = stepForward(charsBefore, c, i);
					}
				}
			} else if (isString) {
				if (c == BACKSLASH) {
					backslashes++;
				} else {
					if (DSLUtils.isStringEnd(charsBefore[0], charsBefore[1], c, backslashes)) {// 字符串区域结束
						isString = false;
					}
					backslashes = 0;
				}
				i = stepForward(charsBefore, c, i);
			} else {
				if (c == SINGLE_QUOTATION_MARK) {// 字符串区域开始
					isString = true;
					i = stepForward(charsBefore, c, i);
				} else {
					if (c == LEFT_BRACKET) {// 左括号
						deep++;
						sb.setLength(0);
					} else if (c == RIGHT_BRACKET) {// 右括号
						deep--;
						sb.setLength(0);
					} else if (c <= BLANK_SPACE) {// 遇到空白字符
						if (deep == 0) {
							if (sqlMetaData.getSelectIndex() < 0) {
								decideLineSplitorIndex(lineSplitorIndexs, c, i);
							}
							String s = sb.toString();
							if (SELECT.equalsIgnoreCase(s)) {
								sqlMetaData.setSelectIndex(i - SELECT_LEN);
							} else if (FROM.equalsIgnoreCase(s)) {
								sqlMetaData.setFromIndex(i - FROM_LEN);
							} else if (WITH.equalsIgnoreCase(s)) {
								sqlMetaData.setWithIndex(i - WITH_LEN);
								isWith = true;
							}
						}
						sb.setLength(0);
					} else {
						sb.append(c);// 拼接单词
					}
					i = stepForward(charsBefore, c, i);
				}
			}
		}
		setEmbedStartIndex(sqlMetaData, lineSplitorIndexs[0], lineSplitorIndexs[1]);
	}

	/**
	 * 向前前进一步
	 * 
	 * @param charsBefore
	 *            当前字符c前部的两个字符
	 * @param c
	 *            当前字符
	 * @param i
	 *            当前字符的位置
	 * @return 返回下一个索引值
	 */
	private static int stepForward(char[] charsBefore, char c, int i) {
		charsBefore[0] = charsBefore[1];
		charsBefore[1] = c;
		return ++i;
	}

	/**
	 * 确定SELECT子句之前最后一个换行符的位置
	 * 
	 * @param lineSplitorIndexs
	 *            SELECT子句之前最后一个换行符的位置
	 * @param c
	 *            当前字符
	 * @param i
	 *            当前字符的位置
	 */
	private static void decideLineSplitorIndex(int[] lineSplitorIndexs, char c, int i) {
		if (c == LINE_SEPARATOR[1]) {
			lineSplitorIndexs[1] = i;
		} else if (c == LINE_SEPARATOR[0]) {
			lineSplitorIndexs[0] = i;
		}
	}

	/**
	 * 设置查询嵌入的开始位置
	 * 
	 * @param sqlMetaData
	 *            SQL相关数据对象
	 * @param r
	 *            /r的索引
	 * @param n
	 *            /n的索引
	 */
	private static void setEmbedStartIndex(SQLMetaData sqlMetaData, int r, int n) {
		int withIndex = sqlMetaData.getWithIndex(), selectIndex = sqlMetaData.getSelectIndex();
		if (selectIndex > 0) {
			if (withIndex >= 0 && selectIndex > withIndex) {
				sqlMetaData.setEmbedStartIndex(selectIndex);
			} else {
				if (r < n && n < selectIndex) {
					sqlMetaData.setEmbedStartIndex(n + 1);
				} else if (r > n && r < selectIndex) {
					sqlMetaData.setEmbedStartIndex(r + 1);
				} else {
					sqlMetaData.setEmbedStartIndex(selectIndex);
				}
			}
		} else {
			sqlMetaData.setEmbedStartIndex(0);
		}
	}

	/**
	 * 设置查询嵌入的结束位置
	 * 
	 * @param sqlMetaData
	 *            SQL相关数据对象
	 * @param r
	 *            /r的索引
	 * @param n
	 *            /n的索引
	 */
	private static void setEmbedEndIndex(SQLMetaData sqlMetaData, int r, int n) {
		if (r < n) {
			sqlMetaData.setEmbedEndIndex(r);
		} else if (r > n) {
			sqlMetaData.setEmbedEndIndex(n);
		} else {
			sqlMetaData.setEmbedEndIndex(sqlMetaData.getLength());
		}
	}

}
