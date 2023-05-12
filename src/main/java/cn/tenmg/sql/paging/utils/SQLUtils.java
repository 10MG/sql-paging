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

	private static final String WITH = "WITH", SELECT = "SELECT", FROM = "FROM", UNION = "UNION", FROM_REVERSE = "MORF",
			ON_REVERSE = "NO", WHERE_REVERSE = "EREHW", GROUP_REVERSE = "PUORG", HAVING_REVERSE = "GNIVAH",
			ORDER_REVERSE = "REDRO", BY_REVERSE = "YB", LIMIT_REVERSE = "TIMIL", OFFSET_REVERSE = "TESFFO",
			FETCH_REVERSE = "HCTEF", AND = "AND", BLANK_SPACE_AND = " " + AND, IMPOSSIBLE = " 1=0",
			WHERE_IMPOSSIBLE = "WHERE" + IMPOSSIBLE, BLANK_SPACE_WHERE_IMPOSSIBLE = BLANK_SPACE + WHERE_IMPOSSIBLE;

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

	/**
	 * 分析SQL右边部分
	 * 
	 * @param sql
	 *            SQL
	 * @param sqlMetaData
	 *            SQL相关数据对象
	 */
	private static void rightAnalysis(String sql, SQLMetaData sqlMetaData) {
		int length = sqlMetaData.getLength(), i = length - 1, deep = 0;
		char c = sql.charAt(i);
		boolean isString = false;
		StringBuilder sba = new StringBuilder(), sbb = new StringBuilder();
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
							} else if (WHERE_REVERSE.equalsIgnoreCase(sb)) {
								sqlMetaData.setWhereIndex(i + 1);
								break;
							} else if (ON_REVERSE.equalsIgnoreCase(sb)) {
								break;
							} else if (FROM_REVERSE.equalsIgnoreCase(sb)) {
								sqlMetaData.setFromIndex(i + 1);
								break;
							} else if (HAVING_REVERSE.equals(sa)) {
								sqlMetaData.setHavingIndex(i + 1);
							} else if (LIMIT_REVERSE.equalsIgnoreCase(sb)) {
								sqlMetaData.setLimitIndex(i + 1);
							} else if (OFFSET_REVERSE.equalsIgnoreCase(sb)) {
								sqlMetaData.setOffsetIndex(i + 1);
							} else if (FETCH_REVERSE.equalsIgnoreCase(sb)) {
								sqlMetaData.setFetchIndex(i + 1);
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
		boolean isString = false, isWith = false;// 是否在字符串区域，是否在WITH子句区域
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
								if (sqlMetaData.getSelectIndex() < 0) {
									sqlMetaData.setSelectIndex(i - SELECT_LEN);
								}
							} else if (FROM.equalsIgnoreCase(s)) {
								if (sqlMetaData.getFromIndex() < 0) {
									sqlMetaData.setFromIndex(i - FROM_LEN);
								}
							} else if (WITH.equalsIgnoreCase(s)) {
								sqlMetaData.setWithIndex(i - WITH_LEN);
								isWith = true;
							} else if (UNION.equalsIgnoreCase(s)) {
								sqlMetaData.setUnion(true);
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
	 * 确定换行符的位置
	 * 
	 * @param lineSplitorIndexs
	 *            换行符的位置
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

	/**
	 * 获取查询SQL指定索引（index）左边最远的不可见字符索引
	 * 
	 * @param sql
	 *            查询SQL
	 * @param index
	 *            指定索引
	 * @return 查询SQL指定索引（index）左边最远的不可见字符索引
	 */
	protected static int leftFarthestBlank(String sql, int index) {
		char c;
		int leftFarthestNotNewline = index, i = index;
		while (i > 0) {
			c = sql.charAt(--i);
			if (c > DSLUtils.BLANK_SPACE) {
				break;
			} else if (c == '\n') {
				leftFarthestNotNewline = i + 1;
				break;
			}
		}
		return leftFarthestNotNewline;
	}

}
