package cn.tenmg.sql.paging.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import cn.tenmg.sql.paging.SQLMetaData;

public class SQLUtilsTest {

	private static final String sql = "/*\r\n" + "SELECT\r\n" + "  *\r\n" + "FROM STAFF_INFO S\r\n"
			+ "WHERE S.STAFF_NAME LIKE :staffName\r\n" + "*/\r\n" + "SELECT\r\n" + "  *\r\n"
			+ "FROM STAFF_INFO S -- SELECT * FROM STAFF_INFO WHEERE STAFF_NAME LIKE :staffName\r\n"
			+ "WHERE #[if(:curDepartmentId == '01') 1=1]\r\n" + "  #[else S.DEPARTMENT_ID = :curDepartmentId]\r\n"
			+ "  #[AND S.STAFF_ID = :staffId]\r\n" + "  #[AND S.STAFF_NAME LIKE :staffName]\r\n" + "/*\r\n"
			+ "SELECT\r\n" + "  *\r\n" + "FROM STAFF_INFO S\r\n" + "WHERE S.STAFF_NAME LIKE :staffName\r\n" + "*/";

	@Test
	public void commentTest() {
		SQLMetaData sqlMetaData = SQLUtils.getSQLMetaData(sql);
		Assertions.assertEquals(
				"SELECT * FROM STAFF_INFO S -- SELECT * FROM STAFF_INFO WHEERE STAFF_NAME LIKE :staffName",
				sql.substring(sqlMetaData.getSelectIndex(), sqlMetaData.getWhereIndex()).trim().replaceAll("[\\s]+",
						" "));
	}

}
