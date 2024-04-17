package cn.tenmg.sql.paging.dialect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import cn.tenmg.sql.paging.SQLPagingDialect;
import cn.tenmg.sql.paging.utils.JDBCUtils;
import cn.tenmg.sql.paging.utils.SQLUtils;

public class PostgreSQLPagingDialectTest extends AbstractPagingDialectTest {

	private static final Properties config = loadConfig("postgresql.properties");

	private static final String SQL = "SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO",
			COUNT_SQL = "SELECT COUNT(*) FROM STAFF_INFO",
			WITH_SQL = "WITH T AS (SELECT STAFF_ID, STAFF_NAME FROM STAFF_INFO WHERE STAFF_NAME LIKE :staffName) SELECT * FROM T",
			WITH_COUNT_SQL = "WITH T AS (SELECT STAFF_ID, STAFF_NAME FROM STAFF_INFO WHERE STAFF_NAME LIKE :staffName) SELECT COUNT(*) FROM T",
			ORDER_BY_SQL = SQL + " ORDER BY STAFF_ID,STAFF_NAME", GROUP_BY_SQL = SQL + " GROUP BY STAFF_ID,STAFF_NAME",
			LIMIT_SQL = SQL + " LIMIT 10",
			GROUP_BY_ORDER_BY_SQL = "SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME ORDER BY STAFF_NAME",
			GROUP_BY_LIMIT_SQL = "SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME LIMIT 10",
			ORDER_BY_LIMIT_SQL = "SELECT STAFF_NAME FROM STAFF_INFO ORDER BY STAFF_NAME LIMIT 10",
			GROUP_BY_ORDER_BY_LIMIT_SQL = "SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME ORDER BY STAFF_NAME LIMIT 10",

			UNION_SQL = "SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO",
			UNION_COUNT_SQL = "SELECT COUNT(*) FROM (" + UNION_SQL + ") SQL_PAGING",
			UNION_ORDER_BY_SQL = UNION_SQL + " ORDER BY STAFF_ID,STAFF_NAME",
			UNION_GROUP_BY_SQL = UNION_SQL + " GROUP BY STAFF_ID,STAFF_NAME", UNION_LIMIT_SQL = UNION_SQL + " LIMIT 10",
			UNION_GROUP_BY_ORDER_BY_SQL = "SELECT STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME ORDER BY STAFF_NAME",
			UNION_GROUP_BY_LIMIT_SQL = "SELECT STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME LIMIT 10",
			UNION_ORDER_BY_LIMIT_SQL = "SELECT STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_NAME FROM STAFF_INFO ORDER BY STAFF_NAME LIMIT 10",
			UNION_GROUP_BY_ORDER_BY_LIMIT_SQL = "SELECT STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME ORDER BY STAFF_NAME LIMIT 10";

	private static final SQLPagingDialect pagingDialect = PostgreSQLPagingDialect.getInstance();

	@Test
	public void countSqlTest() {
		Assertions.assertEquals(COUNT_SQL, pagingDialect.countSql(SQL, SQLUtils.getSQLMetaData(SQL)));

		Assertions.assertEquals(WITH_COUNT_SQL, pagingDialect.countSql(WITH_SQL, SQLUtils.getSQLMetaData(WITH_SQL)));

		Assertions.assertEquals(COUNT_SQL, pagingDialect.countSql(ORDER_BY_SQL, SQLUtils.getSQLMetaData(ORDER_BY_SQL))
				.replaceAll("[\\s]+\\)", ")").trim());

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_ID,STAFF_NAME) SQL_PAGING",
				pagingDialect.countSql(GROUP_BY_SQL, SQLUtils.getSQLMetaData(GROUP_BY_SQL)).replaceAll("[\\s]+\\)",
						")"));

		Assertions.assertEquals("SELECT COUNT(*) FROM (SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO LIMIT 10) SQL_PAGING",
				pagingDialect.countSql(LIMIT_SQL, SQLUtils.getSQLMetaData(LIMIT_SQL)).replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME) SQL_PAGING",
				pagingDialect.countSql(GROUP_BY_ORDER_BY_SQL, SQLUtils.getSQLMetaData(GROUP_BY_ORDER_BY_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME LIMIT 10) SQL_PAGING",
				pagingDialect.countSql(GROUP_BY_LIMIT_SQL, SQLUtils.getSQLMetaData(GROUP_BY_LIMIT_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals("SELECT COUNT(*) FROM (SELECT STAFF_NAME FROM STAFF_INFO LIMIT 10) SQL_PAGING",
				pagingDialect.countSql(ORDER_BY_LIMIT_SQL, SQLUtils.getSQLMetaData(ORDER_BY_LIMIT_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME LIMIT 10) SQL_PAGING",
				pagingDialect
						.countSql(GROUP_BY_ORDER_BY_LIMIT_SQL, SQLUtils.getSQLMetaData(GROUP_BY_ORDER_BY_LIMIT_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(UNION_COUNT_SQL, pagingDialect.countSql(UNION_SQL, SQLUtils.getSQLMetaData(UNION_SQL)));

		Assertions.assertEquals(UNION_COUNT_SQL,
				pagingDialect.countSql(UNION_ORDER_BY_SQL, SQLUtils.getSQLMetaData(UNION_ORDER_BY_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_ID,STAFF_NAME) SQL_PAGING",
				pagingDialect.countSql(UNION_GROUP_BY_SQL, SQLUtils.getSQLMetaData(UNION_GROUP_BY_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO LIMIT 10) SQL_PAGING",
				pagingDialect.countSql(UNION_LIMIT_SQL, SQLUtils.getSQLMetaData(UNION_LIMIT_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME) SQL_PAGING",
				pagingDialect
						.countSql(UNION_GROUP_BY_ORDER_BY_SQL, SQLUtils.getSQLMetaData(UNION_GROUP_BY_ORDER_BY_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME LIMIT 10) SQL_PAGING",
				pagingDialect.countSql(UNION_GROUP_BY_LIMIT_SQL, SQLUtils.getSQLMetaData(UNION_GROUP_BY_LIMIT_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_NAME FROM STAFF_INFO LIMIT 10) SQL_PAGING",
				pagingDialect.countSql(UNION_ORDER_BY_LIMIT_SQL, SQLUtils.getSQLMetaData(UNION_ORDER_BY_LIMIT_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME LIMIT 10) SQL_PAGING",
				pagingDialect
						.countSql(UNION_GROUP_BY_ORDER_BY_LIMIT_SQL,
								SQLUtils.getSQLMetaData(UNION_GROUP_BY_ORDER_BY_LIMIT_SQL))
						.replaceAll("[\\s]+\\)", ")"));
	}

	@Test
	public void pageSqlTest() throws SQLException {
		Connection con = null;
		try {
			con = DriverManager.getConnection(config.getProperty("url"), config.getProperty("username"),
					config.getProperty("password"));
			Assertions.assertEquals(SQL + " LIMIT 10 OFFSET 0",
					pagingDialect.pageSql(con, SQL, null, SQLUtils.getSQLMetaData(SQL), 10, 1));

			Assertions.assertEquals(WITH_SQL + " LIMIT 10 OFFSET 0",
					pagingDialect.pageSql(con, WITH_SQL, null, SQLUtils.getSQLMetaData(WITH_SQL), 10, 1));

			Assertions.assertEquals(ORDER_BY_SQL + " LIMIT 10 OFFSET 0",
					pagingDialect.pageSql(con, ORDER_BY_SQL, null, SQLUtils.getSQLMetaData(ORDER_BY_SQL), 10, 1));

			Assertions.assertEquals(GROUP_BY_SQL + " LIMIT 10 OFFSET 0",
					pagingDialect.pageSql(con, GROUP_BY_SQL, null, SQLUtils.getSQLMetaData(GROUP_BY_SQL), 10, 1));

			Assertions.assertEquals("SELECT * FROM (" + LIMIT_SQL + ") SQL_PAGING LIMIT 10 OFFSET 0",
					pagingDialect.pageSql(con, LIMIT_SQL, null, SQLUtils.getSQLMetaData(LIMIT_SQL), 10, 1));

			Assertions.assertEquals(GROUP_BY_ORDER_BY_SQL + " LIMIT 10 OFFSET 0", pagingDialect.pageSql(con,
					GROUP_BY_ORDER_BY_SQL, null, SQLUtils.getSQLMetaData(GROUP_BY_ORDER_BY_SQL), 10, 1));

			Assertions.assertEquals("SELECT * FROM (" + GROUP_BY_LIMIT_SQL + ") SQL_PAGING LIMIT 10 OFFSET 0",
					pagingDialect.pageSql(con, GROUP_BY_LIMIT_SQL, null, SQLUtils.getSQLMetaData(GROUP_BY_LIMIT_SQL),
							10, 1));

			Assertions.assertEquals("SELECT * FROM (" + ORDER_BY_LIMIT_SQL + ") SQL_PAGING LIMIT 10 OFFSET 0",
					pagingDialect.pageSql(con, ORDER_BY_LIMIT_SQL, null, SQLUtils.getSQLMetaData(ORDER_BY_LIMIT_SQL),
							10, 1));

			Assertions.assertEquals("SELECT * FROM (" + GROUP_BY_ORDER_BY_LIMIT_SQL + ") SQL_PAGING LIMIT 10 OFFSET 0",
					pagingDialect.pageSql(con, GROUP_BY_ORDER_BY_LIMIT_SQL, null,
							SQLUtils.getSQLMetaData(GROUP_BY_ORDER_BY_LIMIT_SQL), 10, 1));

			Assertions.assertEquals(UNION_SQL + " LIMIT 10 OFFSET 0",
					pagingDialect.pageSql(con, UNION_SQL, null, SQLUtils.getSQLMetaData(UNION_SQL), 10, 1));

			Assertions.assertEquals(UNION_ORDER_BY_SQL + " LIMIT 10 OFFSET 0", pagingDialect.pageSql(con,
					UNION_ORDER_BY_SQL, null, SQLUtils.getSQLMetaData(UNION_ORDER_BY_SQL), 10, 1));

			Assertions.assertEquals(UNION_GROUP_BY_SQL + " LIMIT 10 OFFSET 0", pagingDialect.pageSql(con,
					UNION_GROUP_BY_SQL, null, SQLUtils.getSQLMetaData(UNION_GROUP_BY_SQL), 10, 1));

			Assertions.assertEquals("SELECT * FROM (" + UNION_LIMIT_SQL + ") SQL_PAGING LIMIT 10 OFFSET 0",
					pagingDialect.pageSql(con, UNION_LIMIT_SQL, null, SQLUtils.getSQLMetaData(UNION_LIMIT_SQL), 10, 1));

			Assertions.assertEquals(UNION_GROUP_BY_ORDER_BY_SQL + " LIMIT 10 OFFSET 0", pagingDialect.pageSql(con,
					UNION_GROUP_BY_ORDER_BY_SQL, null, SQLUtils.getSQLMetaData(UNION_GROUP_BY_ORDER_BY_SQL), 10, 1));

			Assertions.assertEquals("SELECT * FROM (" + UNION_GROUP_BY_LIMIT_SQL + ") SQL_PAGING LIMIT 10 OFFSET 0",
					pagingDialect.pageSql(con, UNION_GROUP_BY_LIMIT_SQL, null,
							SQLUtils.getSQLMetaData(UNION_GROUP_BY_LIMIT_SQL), 10, 1));

			Assertions.assertEquals("SELECT * FROM (" + UNION_ORDER_BY_LIMIT_SQL + ") SQL_PAGING LIMIT 10 OFFSET 0",
					pagingDialect.pageSql(con, UNION_ORDER_BY_LIMIT_SQL, null,
							SQLUtils.getSQLMetaData(UNION_ORDER_BY_LIMIT_SQL), 10, 1));

			Assertions.assertEquals(
					"SELECT * FROM (" + UNION_GROUP_BY_ORDER_BY_LIMIT_SQL + ") SQL_PAGING LIMIT 10 OFFSET 0",
					pagingDialect.pageSql(con, UNION_GROUP_BY_ORDER_BY_LIMIT_SQL, null,
							SQLUtils.getSQLMetaData(UNION_GROUP_BY_ORDER_BY_LIMIT_SQL), 10, 1));
		} finally {
			JDBCUtils.close(con);
		}
	}
}
