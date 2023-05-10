package cn.tenmg.sql.paging;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import cn.tenmg.dsl.utils.PropertiesLoaderUtils;
import cn.tenmg.sql.paging.dialect.SQLServerPagingDialect;
import cn.tenmg.sql.paging.utils.JDBCUtils;
import cn.tenmg.sql.paging.utils.SQLUtils;

public class SQLServerPagingDialectTest {

	private static final Properties config = PropertiesLoaderUtils.loadIgnoreException("sqlserver.properties");

	private static final String SQL = "SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO",
			COUNT_SQL = "SELECT COUNT(*) FROM (" + SQL + ") SQL_PAGING",
			ORDER_BY_SQL = SQL + " ORDER BY STAFF_ID,STAFF_NAME", GROUP_BY_SQL = SQL + " GROUP BY STAFF_ID,STAFF_NAME",
			LIMIT_SQL = SQL + " OFFSET 0 ROW FETCH NEXT 10 ROW ONLY";

	private static final SQLPagingDialect pagingDialect = SQLServerPagingDialect.getInstance();

	@Test
	public void countSqlTest() {
		Assertions.assertEquals(COUNT_SQL, pagingDialect.countSql(SQL, SQLUtils.getSQLMetaData(SQL)));

		Assertions.assertEquals("SELECT COUNT(*) FROM (" + SQL + ") SQL_PAGING", pagingDialect
				.countSql(ORDER_BY_SQL, SQLUtils.getSQLMetaData(ORDER_BY_SQL)).replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_ID,STAFF_NAME) SQL_PAGING",
				pagingDialect.countSql(GROUP_BY_SQL, SQLUtils.getSQLMetaData(GROUP_BY_SQL)).replaceAll("[\\s]+\\)",
						")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT 1 RN__, STAFF_ID,STAFF_NAME FROM STAFF_INFO UNION ALL SELECT 1 RN__, STAFF_ID,STAFF_NAME FROM STAFF_INFO ORDER BY RN__ OFFSET 0 ROW FETCH NEXT 10 ROW ONLY) SQL_PAGING",
				pagingDialect.countSql(LIMIT_SQL, SQLUtils.getSQLMetaData(LIMIT_SQL)).replaceAll("[\\s]+\\)", ")"));
	}

	@Test
	public void pageSqlTest() throws SQLException, ClassNotFoundException {
		Connection con = null;
		try {
			con = DriverManager.getConnection(config.getProperty("url"), config.getProperty("username"),
					config.getProperty("password"));
			Assertions.assertEquals(
					"SELECT STAFF_ID, STAFF_NAME FROM (SELECT 1 RN__, STAFF_ID,STAFF_NAME FROM STAFF_INFO UNION ALL SELECT 1 RN__, STAFF_ID,STAFF_NAME FROM STAFF_INFO ORDER BY RN__ OFFSET 0 ROW FETCH NEXT 10 ROW ONLY) SQL_PAGING",
					pagingDialect.pageSql(con, SQL, null, SQLUtils.getSQLMetaData(SQL), 10, 1));

			Assertions.assertEquals(ORDER_BY_SQL + " OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
					pagingDialect.pageSql(con, ORDER_BY_SQL, null, SQLUtils.getSQLMetaData(ORDER_BY_SQL), 10, 1));

			Assertions.assertEquals(
					"SELECT STAFF_ID, STAFF_NAME FROM (SELECT 1 RN__, STAFF_ID,STAFF_NAME FROM STAFF_INFO UNION ALL SELECT 1 RN__, STAFF_ID,STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_ID,STAFF_NAME ORDER BY RN__ OFFSET 0 ROW FETCH NEXT 10 ROW ONLY) SQL_PAGING",
					pagingDialect.pageSql(con, GROUP_BY_SQL, null, SQLUtils.getSQLMetaData(GROUP_BY_SQL), 10, 1));

			Assertions.assertEquals(
					"SELECT STAFF_ID, STAFF_NAME FROM (SELECT 1 RN__, STAFF_ID,STAFF_NAME FROM STAFF_INFO UNION ALL SELECT 1 RN__, STAFF_ID,STAFF_NAME FROM STAFF_INFO ORDER BY RN__ OFFSET 0 ROW FETCH NEXT 10 ROW ONLY) SQL_PAGING ORDER BY RN__ OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
					pagingDialect.pageSql(con, LIMIT_SQL, null, SQLUtils.getSQLMetaData(LIMIT_SQL), 10, 1));
		} finally {
			JDBCUtils.close(con);
		}
	}
}
