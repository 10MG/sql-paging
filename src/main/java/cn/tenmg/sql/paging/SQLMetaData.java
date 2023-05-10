package cn.tenmg.sql.paging;

/**
 * SQL相关数据
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.0.0
 *
 */
public class SQLMetaData {

	/**
	 * WITH子句的位置
	 */
	private int withIndex = -1;

	/**
	 * SELECT子句的位置
	 */
	private int selectIndex = -1;

	/**
	 * FROM子句的位置
	 */
	private int fromIndex = -1;

	/**
	 * WHER子句的位置
	 */
	private int whereIndex = -1;

	/**
	 * 主查询GROUP BY子句索引
	 */
	private int groupByIndex = -1;
	
	/**
	 * 主查询HAVING子句索引
	 */
	private int havingIndex = -1;

	/**
	 * 主查询ORDER BY子句索引
	 */
	private int orderByIndex = -1;

	/**
	 * 主查询LIMIT子句索引
	 */
	private int limitIndex = -1;

	/**
	 * 主查询OFFSET子句索引
	 */
	private int offsetIndex = -1;

	/**
	 * 主查询FETCH子句索引
	 */
	private int fetchIndex = -1;

	/**
	 * 主查询是否含有 UNION 子句
	 */
	private boolean union = false;

	/**
	 * SQL的长度
	 */
	private int length = 0;

	public int getWithIndex() {
		return withIndex;
	}

	public void setWithIndex(int withIndex) {
		this.withIndex = withIndex;
	}

	public int getSelectIndex() {
		return selectIndex;
	}

	public void setSelectIndex(int selectIndex) {
		this.selectIndex = selectIndex;
	}

	public int getFromIndex() {
		return fromIndex;
	}

	public void setFromIndex(int fromIndex) {
		this.fromIndex = fromIndex;
	}

	public int getWhereIndex() {
		return whereIndex;
	}

	public void setWhereIndex(int whereIndex) {
		this.whereIndex = whereIndex;
	}

	public int getGroupByIndex() {
		return groupByIndex;
	}

	public void setGroupByIndex(int groupByIndex) {
		this.groupByIndex = groupByIndex;
	}

	public int getHavingIndex() {
		return havingIndex;
	}

	public void setHavingIndex(int havingIndex) {
		this.havingIndex = havingIndex;
	}

	public int getOrderByIndex() {
		return orderByIndex;
	}

	public void setOrderByIndex(int orderByIndex) {
		this.orderByIndex = orderByIndex;
	}

	public int getLimitIndex() {
		return limitIndex;
	}

	public void setLimitIndex(int limitIndex) {
		this.limitIndex = limitIndex;
	}

	public int getOffsetIndex() {
		return offsetIndex;
	}

	public void setOffsetIndex(int offsetIndex) {
		this.offsetIndex = offsetIndex;
	}

	public int getFetchIndex() {
		return fetchIndex;
	}

	public void setFetchIndex(int fetchIndex) {
		this.fetchIndex = fetchIndex;
	}

	public boolean isUnion() {
		return union;
	}

	public void setUnion(boolean union) {
		this.union = union;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

}
