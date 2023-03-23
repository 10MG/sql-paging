package cn.tenmg.sql.paging.dialect;

/**
 * SQLite分页查询方言
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.0.0
 */
public class SQLitePagingDialect extends MySQLPagingDialect {

	private static final SQLitePagingDialect INSTANCE = new SQLitePagingDialect();

	public static SQLitePagingDialect getInstance() {
		return INSTANCE;
	}

	protected SQLitePagingDialect() {
		super();
	}
}
