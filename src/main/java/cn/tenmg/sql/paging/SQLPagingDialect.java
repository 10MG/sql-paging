package cn.tenmg.sql.paging;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * SQL分页查询方言
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.0.0
 */
public interface SQLPagingDialect {

	/**
	 * 根据SQL生成特定数据库统计总记录数SQL
	 * 
	 * @param sql
	 *            查询SQL
	 * @param sqlMetaData
	 *            SQL相关数据
	 * @return 返回查询总记录数的SQL
	 */
	String countSql(String sql, SQLMetaData sqlMetaData);

	/**
	 * 根据SQL、页容量pageSize和当前页码currentPage生成特定数据库的分页查询SQL
	 * 
	 * @param con
	 *            已开启的数据库连接
	 * @param sql
	 *            SQL
	 * @param params
	 *            查询参数集
	 * @param sqlMetaData
	 *            SQL相关数据
	 * @param pageSize
	 *            页容量
	 * @param currentPage
	 *            当前页码
	 * @return 返回分页查询SQL
	 * @throws SQLException
	 *             SQL异常
	 */
	String pageSql(Connection con, String sql, Map<String, ?> params, SQLMetaData sqlMetaData, int pageSize,
			long currentPage) throws SQLException;

}