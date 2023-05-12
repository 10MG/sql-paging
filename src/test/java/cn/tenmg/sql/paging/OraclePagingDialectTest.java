package cn.tenmg.sql.paging;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import cn.tenmg.dsl.utils.PropertiesLoaderUtils;
import cn.tenmg.sql.paging.dialect.OraclePagingDialect;
import cn.tenmg.sql.paging.utils.JDBCUtils;
import cn.tenmg.sql.paging.utils.SQLUtils;

public class OraclePagingDialectTest {

	private static final Properties config = PropertiesLoaderUtils.loadIgnoreException("oracle.properties");

	private static final String SQL = "SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO",
			COUNT_SQL = "SELECT COUNT(*) FROM STAFF_INFO", ORDER_BY_SQL = SQL + " ORDER BY STAFF_ID,STAFF_NAME",
			GROUP_BY_SQL = SQL + " GROUP BY STAFF_ID,STAFF_NAME",
			LIMIT_SQL = SQL + " OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
			GROUP_BY_ORDER_BY_SQL = "SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME ORDER BY STAFF_NAME",
			GROUP_BY_LIMIT_SQL = "SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
			ORDER_BY_LIMIT_SQL = "SELECT STAFF_NAME FROM STAFF_INFO ORDER BY STAFF_NAME OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
			GROUP_BY_ORDER_BY_LIMIT_SQL = "SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME ORDER BY STAFF_NAME OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",

			UNION_SQL = "SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO",
			UNION_COUNT_SQL = "SELECT COUNT(*) FROM (" + UNION_SQL + ") SQL_PAGING",
			UNION_ORDER_BY_SQL = UNION_SQL + " ORDER BY STAFF_ID,STAFF_NAME",
			UNION_GROUP_BY_SQL = UNION_SQL + " GROUP BY STAFF_ID,STAFF_NAME",
			UNION_LIMIT_SQL = UNION_SQL + " OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
			UNION_GROUP_BY_ORDER_BY_SQL = "SELECT STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME ORDER BY STAFF_NAME",
			UNION_GROUP_BY_LIMIT_SQL = "SELECT STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
			UNION_ORDER_BY_LIMIT_SQL = "SELECT STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_NAME FROM STAFF_INFO ORDER BY STAFF_NAME OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
			UNION_GROUP_BY_ORDER_BY_LIMIT_SQL = "SELECT STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME ORDER BY STAFF_NAME OFFSET 0 ROW FETCH NEXT 10 ROW ONLY";;

	private static final SQLPagingDialect pagingDialect = OraclePagingDialect.getInstance();

	@Test
	public void countSqlTest() {
		Assertions.assertEquals(COUNT_SQL, pagingDialect.countSql(SQL, SQLUtils.getSQLMetaData(SQL)));

		Assertions.assertEquals(COUNT_SQL, pagingDialect.countSql(ORDER_BY_SQL, SQLUtils.getSQLMetaData(ORDER_BY_SQL))
				.replaceAll("[\\s]+\\)", ")").trim());

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_ID,STAFF_NAME) SQL_PAGING",
				pagingDialect.countSql(GROUP_BY_SQL, SQLUtils.getSQLMetaData(GROUP_BY_SQL)).replaceAll("[\\s]+\\)",
						")"));

		Assertions.assertEquals("SELECT COUNT(*) FROM (" + LIMIT_SQL + ") SQL_PAGING",
				pagingDialect.countSql(LIMIT_SQL, SQLUtils.getSQLMetaData(LIMIT_SQL)).replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME) SQL_PAGING",
				pagingDialect.countSql(GROUP_BY_ORDER_BY_SQL, SQLUtils.getSQLMetaData(GROUP_BY_ORDER_BY_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME OFFSET 0 ROW FETCH NEXT 10 ROW ONLY) SQL_PAGING",
				pagingDialect.countSql(GROUP_BY_LIMIT_SQL, SQLUtils.getSQLMetaData(GROUP_BY_LIMIT_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_NAME FROM STAFF_INFO OFFSET 0 ROW FETCH NEXT 10 ROW ONLY) SQL_PAGING",
				pagingDialect.countSql(ORDER_BY_LIMIT_SQL, SQLUtils.getSQLMetaData(ORDER_BY_LIMIT_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME OFFSET 0 ROW FETCH NEXT 10 ROW ONLY) SQL_PAGING",
				pagingDialect
						.countSql(GROUP_BY_ORDER_BY_LIMIT_SQL, SQLUtils.getSQLMetaData(GROUP_BY_ORDER_BY_LIMIT_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(UNION_COUNT_SQL, pagingDialect.countSql(UNION_SQL, SQLUtils.getSQLMetaData(UNION_SQL)));

		Assertions.assertEquals("SELECT COUNT(*) FROM (" + UNION_SQL + ") SQL_PAGING",
				pagingDialect.countSql(UNION_ORDER_BY_SQL, SQLUtils.getSQLMetaData(UNION_ORDER_BY_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_ID,STAFF_NAME) SQL_PAGING",
				pagingDialect.countSql(UNION_GROUP_BY_SQL, SQLUtils.getSQLMetaData(UNION_GROUP_BY_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO OFFSET 0 ROW FETCH NEXT 10 ROW ONLY) SQL_PAGING",
				pagingDialect.countSql(UNION_LIMIT_SQL, SQLUtils.getSQLMetaData(UNION_LIMIT_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME) SQL_PAGING",
				pagingDialect
						.countSql(UNION_GROUP_BY_ORDER_BY_SQL, SQLUtils.getSQLMetaData(UNION_GROUP_BY_ORDER_BY_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME OFFSET 0 ROW FETCH NEXT 10 ROW ONLY) SQL_PAGING",
				pagingDialect.countSql(UNION_GROUP_BY_LIMIT_SQL, SQLUtils.getSQLMetaData(UNION_GROUP_BY_LIMIT_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_NAME FROM STAFF_INFO OFFSET 0 ROW FETCH NEXT 10 ROW ONLY) SQL_PAGING",
				pagingDialect.countSql(UNION_ORDER_BY_LIMIT_SQL, SQLUtils.getSQLMetaData(UNION_ORDER_BY_LIMIT_SQL))
						.replaceAll("[\\s]+\\)", ")"));

		Assertions.assertEquals(
				"SELECT COUNT(*) FROM (SELECT STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME OFFSET 0 ROW FETCH NEXT 10 ROW ONLY) SQL_PAGING",
				pagingDialect
						.countSql(UNION_GROUP_BY_ORDER_BY_LIMIT_SQL,
								SQLUtils.getSQLMetaData(UNION_GROUP_BY_ORDER_BY_LIMIT_SQL))
						.replaceAll("[\\s]+\\)", ")"));
	}

	@Test
	public void pageSqlTest() throws SQLException, ClassNotFoundException {
		Connection con = null;
		try {
			con = DriverManager.getConnection(config.getProperty("url"), config.getProperty("username"),
					config.getProperty("password"));
			Assertions.assertEquals(SQL + " OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
					pagingDialect.pageSql(con, SQL, null, SQLUtils.getSQLMetaData(SQL), 10, 1));

			Assertions.assertEquals(ORDER_BY_SQL + " OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
					pagingDialect.pageSql(con, ORDER_BY_SQL, null, SQLUtils.getSQLMetaData(ORDER_BY_SQL), 10, 1));

			Assertions.assertEquals(GROUP_BY_SQL + " OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
					pagingDialect.pageSql(con, GROUP_BY_SQL, null, SQLUtils.getSQLMetaData(GROUP_BY_SQL), 10, 1));

			Assertions.assertEquals("SELECT * FROM (" + LIMIT_SQL + ") SQL_PAGING OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
					pagingDialect.pageSql(con, LIMIT_SQL, null, SQLUtils.getSQLMetaData(LIMIT_SQL), 10, 1));

			Assertions.assertEquals(GROUP_BY_ORDER_BY_SQL + " OFFSET 0 ROW FETCH NEXT 10 ROW ONLY", pagingDialect
					.pageSql(con, GROUP_BY_ORDER_BY_SQL, null, SQLUtils.getSQLMetaData(GROUP_BY_ORDER_BY_SQL), 10, 1));

			Assertions.assertEquals(
					"SELECT * FROM (" + GROUP_BY_LIMIT_SQL + ") SQL_PAGING OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
					pagingDialect.pageSql(con, GROUP_BY_LIMIT_SQL, null, SQLUtils.getSQLMetaData(GROUP_BY_LIMIT_SQL),
							10, 1));

			Assertions.assertEquals(
					"SELECT * FROM (" + ORDER_BY_LIMIT_SQL + ") SQL_PAGING OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
					pagingDialect.pageSql(con, ORDER_BY_LIMIT_SQL, null, SQLUtils.getSQLMetaData(ORDER_BY_LIMIT_SQL),
							10, 1));

			Assertions.assertEquals(
					"SELECT * FROM (" + GROUP_BY_ORDER_BY_LIMIT_SQL
							+ ") SQL_PAGING OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
					pagingDialect.pageSql(con, GROUP_BY_ORDER_BY_LIMIT_SQL, null,
							SQLUtils.getSQLMetaData(GROUP_BY_ORDER_BY_LIMIT_SQL), 10, 1));

			Assertions.assertEquals(
					"SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
					pagingDialect.pageSql(con, UNION_SQL, null, SQLUtils.getSQLMetaData(UNION_SQL), 10, 1));

			Assertions.assertEquals(UNION_ORDER_BY_SQL + " OFFSET 0 ROW FETCH NEXT 10 ROW ONLY", pagingDialect
					.pageSql(con, UNION_ORDER_BY_SQL, null, SQLUtils.getSQLMetaData(UNION_ORDER_BY_SQL), 10, 1));

			Assertions.assertEquals(
					"SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_ID,STAFF_NAME OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
					pagingDialect.pageSql(con, UNION_GROUP_BY_SQL, null, SQLUtils.getSQLMetaData(UNION_GROUP_BY_SQL),
							10, 1));

			Assertions.assertEquals(
					"SELECT * FROM (SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_ID,STAFF_NAME FROM STAFF_INFO OFFSET 0 ROW FETCH NEXT 10 ROW ONLY) SQL_PAGING OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
					pagingDialect.pageSql(con, UNION_LIMIT_SQL, null, SQLUtils.getSQLMetaData(UNION_LIMIT_SQL), 10, 1));

			Assertions.assertEquals(UNION_GROUP_BY_ORDER_BY_SQL + " OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
					pagingDialect.pageSql(con, UNION_GROUP_BY_ORDER_BY_SQL, null,
							SQLUtils.getSQLMetaData(UNION_GROUP_BY_ORDER_BY_SQL), 10, 1));

			Assertions.assertEquals(
					"SELECT * FROM (SELECT STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME OFFSET 0 ROW FETCH NEXT 10 ROW ONLY) SQL_PAGING OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
					pagingDialect.pageSql(con, UNION_GROUP_BY_LIMIT_SQL, null,
							SQLUtils.getSQLMetaData(UNION_GROUP_BY_LIMIT_SQL), 10, 1));

			Assertions.assertEquals(
					"SELECT * FROM (SELECT STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_NAME FROM STAFF_INFO ORDER BY STAFF_NAME OFFSET 0 ROW FETCH NEXT 10 ROW ONLY) SQL_PAGING OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
					pagingDialect.pageSql(con, UNION_ORDER_BY_LIMIT_SQL, null,
							SQLUtils.getSQLMetaData(UNION_ORDER_BY_LIMIT_SQL), 10, 1));

			Assertions.assertEquals(
					"SELECT * FROM (SELECT STAFF_NAME FROM STAFF_INFO UNION ALL SELECT STAFF_NAME FROM STAFF_INFO GROUP BY STAFF_NAME ORDER BY STAFF_NAME OFFSET 0 ROW FETCH NEXT 10 ROW ONLY) SQL_PAGING OFFSET 0 ROW FETCH NEXT 10 ROW ONLY",
					pagingDialect.pageSql(con, UNION_GROUP_BY_ORDER_BY_LIMIT_SQL, null,
							SQLUtils.getSQLMetaData(UNION_GROUP_BY_ORDER_BY_LIMIT_SQL), 10, 1));
		} finally {
			JDBCUtils.close(con);
		}
	}
}
