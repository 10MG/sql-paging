package cn.tenmg.sql.paging.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class JDBCUtils {

	private static final Logger log = LogManager.getLogger(JDBCUtils.class);

	private JDBCUtils() {
	}

	public static void close(Connection connection) {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException ex) {
				if (log.isErrorEnabled()) {
					log.error("Could not close JDBC Connection", ex);
				}
				ex.printStackTrace();
			} catch (Throwable ex) {
				if (log.isErrorEnabled()) {
					log.error("Unexpected exception on closing JDBC Connection", ex);
				}
				ex.printStackTrace();
			}
		}
	}

	public static void close(Statement statement) {
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException ex) {
				if (log.isErrorEnabled()) {
					log.error("Could not close JDBC Statement", ex);
				}
				ex.printStackTrace();
			} catch (Throwable ex) {
				if (log.isErrorEnabled()) {
					log.error("Unexpected exception on closing JDBC Statement", ex);
				}
				ex.printStackTrace();
			}
		}
	}

	public static void close(ResultSet resultSet) {
		if (resultSet != null) {
			try {
				resultSet.close();
			} catch (SQLException ex) {
				if (log.isErrorEnabled()) {
					log.error("Could not close JDBC ResultSet", ex);
				}
			} catch (Throwable ex) {
				if (log.isErrorEnabled()) {
					log.error("Unexpected exception on closing JDBC ResultSet", ex);
				}
			}
		}
	}

	/**
	 * 设置参数
	 * 
	 * @param ps
	 *            SQL声明对象
	 * @param params
	 *            查询参数
	 * @throws SQLException
	 */
	public static void setParams(PreparedStatement ps, List<Object> params) throws SQLException {
		if (params == null || params.isEmpty()) {
			return;
		}
		for (int i = 0, size = params.size(); i < size; i++) {
			ps.setObject(i + 1, params.get(i));
		}
	}

}
