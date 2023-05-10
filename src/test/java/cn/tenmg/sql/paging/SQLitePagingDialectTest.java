package cn.tenmg.sql.paging;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import cn.tenmg.dsl.utils.PropertiesLoaderUtils;
import cn.tenmg.sql.paging.dialect.SQLitePagingDialect;
import cn.tenmg.sql.paging.utils.JDBCUtils;
import cn.tenmg.sql.paging.utils.SQLUtils;

public class SQLitePagingDialectTest {

	private static final Properties config = PropertiesLoaderUtils.loadIgnoreException("sqlite.properties");

	private static final String SQL = "SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO",
			COUNT_SQL = "SELECT COUNT(*) FROM (" + SQL + ") SQL_PAGING",
			ORDER_BY_SQL = SQL + " ORDER BY STAFF_ID,STAFF_NAME", GROUP_BY_SQL = SQL + " GROUP BY STAFF_ID,STAFF_NAME",
			LIMIT_SQL = SQL + " LIMIT 10";

	private static final SQLPagingDialect pagingDialect = SQLitePagingDialect.getInstance();

	@Test
	public void countSqlTest() {
		Assertions.assertEquals(COUNT_SQL, pagingDialect.countSql(SQL, SQLUtils.getSQLMetaData(SQL)));

		Assertions.assertEquals(COUNT_SQL, pagingDialect.countSql(ORDER_BY_SQL, SQLUtils.getSQLMetaData(ORDER_BY_SQL))
				.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_ID,STAFF_NAME) SQL_PAGING",
				pagingDialect.countSql(GROUP_BY_SQL, SQLUtils.getSQLMetaData(GROUP_BY_SQL)).replaceAll("[\\s]+\\)",
						")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO LIMIT 10) SQL_PAGING",
				pagingDialect.countSql(LIMIT_SQL, SQLUtils.getSQLMetaData(LIMIT_SQL)).replaceAll("[\\s]+\\)", ")"));
	}

	@Test
	public void pageSqlTest() throws SQLException {
		Connection con = null;
		try {
			con = DriverManager.getConnection(config.getProperty("url"), config.getProperty("username"),
					config.getProperty("password"));
			Assertions.assertEquals(SQL + " LIMIT 0,10",
					pagingDialect.pageSql(con, SQL, null, SQLUtils.getSQLMetaData(SQL), 10, 1));

			Assertions.assertEquals(ORDER_BY_SQL + " LIMIT 0,10",
					pagingDialect.pageSql(con, ORDER_BY_SQL, null, SQLUtils.getSQLMetaData(ORDER_BY_SQL), 10, 1));

			Assertions.assertEquals(GROUP_BY_SQL + " LIMIT 0,10",
					pagingDialect.pageSql(con, GROUP_BY_SQL, null, SQLUtils.getSQLMetaData(GROUP_BY_SQL), 10, 1));

			Assertions.assertEquals("SELECT * FROM (" + LIMIT_SQL + ") SQL_PAGING LIMIT 0,10",
					pagingDialect.pageSql(con, LIMIT_SQL, null, SQLUtils.getSQLMetaData(LIMIT_SQL), 10, 1));
		} finally {
			JDBCUtils.close(con);
		}
	}
}
