# sql-paging

## 介绍
这是一个SQL分页查询方言类库。通过调用相关API，可快速将一个普通SQL转换为一个特定数据库的计数（`COUNT`）SQL或分页查询SQL。通过内置的SQL分析工具类分析实际调用的SQL，方言会生成最优的计数（`COUNT`）SQL或分页查询SQL。

## 数据库支持

数据库     | 支持版本 |     方言实现类
-----------|---------|-------------------------
MySQL      | 1.0+    | MySQLPagingDialect
Oracle     | 1.0+    | OraclePagingDialect
PostgreSQL | 1.0+    | PostgreSQLPagingDialect
SQLServer  | 1.0+    | SQLServerPagingDialect

## 使用说明

以基于Maven项目为例

1.  pom.xml添加依赖，${sql-paging.version}为版本号，可定义属性或直接使用版本号替换

```
<!-- https://mvnrepository.com/artifact/cn.tenmg/sql-paging -->
<dependency>
    <groupId>cn.tenmg</groupId>
    <artifactId>sql-paging</artifactId>
    <version>${sql-paging.version}</version>
</dependency>
```

2.  调用`PagingDialect.countSql`方法获取计数SQL（以MySQL数据库为例）

```
String namedSql = "……"；
sqlMetaData sqlMetaData = SQLUtils.getSQLMetaData(namedSql);
SQLPagingDialect dialect = MySQLPagingDialect.getInstance();
String countSql = dialect.countSql(namedSql, sqlMetaData);
……
```

3.  调用`PagingDialect.pageSql`方法获取分页查询SQL（以MySQL数据库为例）

```
……
try {
    String pageSql = dialect.pageSql(con, namedSql, params, sqlMetaData, 20, 2);
} catch (SQLException e) {
    // TODO Auto-generated catch block
    e.printStackTrace();
}
……
```

## API详解

### countSQL

用于根据实际查询的SQL自动生成计数SQL，完成对总数的统计，结合页容量可计算出总页数。根据对源SQL的分析和智能决策，生成计数SQL会去除不必要的列或者排序子句（ORDER BY），且不会引入不必要子查询，以达到最优性能。例如如下SQL：

```
SELECT
  S.STAFF_ID,
  S.STAFF_NAME,
  S.DEPARTMENT_ID,
  S.POSITION,
  S.STATUS
FROM STAFF_INFO S
ORDER BY S.STAFF_ID
```

并不是简单包裹子查询实现计数：

```
SELECT
  COUNT(*)
FROM (
  SELECT
    S.STAFF_ID,
    S.STAFF_NAME,
    S.DEPARTMENT_ID,
    S.POSITION,
    S.STATUS
  FROM STAFF_INFO S
  ORDER BY S.STAFF_ID
) T
```

而是，不嵌套不必要的子查询，并去除不必要的排序子句：

```
SELECT
  COUNT(*)
FROM STAFF_INFO S
```

嗯，这的确是我们想要的样子。但如果情况复杂一点呢？比如，我们需要查询某段时间内用户的订单金额并按金额从大到小排序：

```
SELECT
  USER_ID,
  SUM(AMT) AMT
FROM ORDER_INFO O
WHERE O.CREATE_TIME >= :begin AND O.CREATE_TIME < :end
GROUP BY USER_ID
ORDER BY SUM(AMT) DESC
```

我们得到的是：

```
SELECT
  COUNT(*)
FROM (
  SELECT
    USER_ID
  FROM ORDER_INFO O
  WHERE O.CREATE_TIME >= :begin AND O.CREATE_TIME < :end
  GROUP BY USER_ID
) SQLTOOL
```

干得漂亮！这完全是我们所期待的。但如果情况再复杂一点呢？比如这样，我们需要查询某段时间内订单金额前一百名的用户：

```
SELECT
  USER_ID, /*用户编号*/
  AMT      /*订单金额*/
FROM (
  SELECT
    USER_ID,
    SUM(AMT) AMT
  FROM ORDER_INFO O
  WHERE O.CREATE_TIME >= :begin AND O.CREATE_TIME < :end
  GROUP BY USER_ID
) T
ORDER BY AMT DESC
LIMIT 100
```

我们得到的是：

```
SELECT
  COUNT(*)
FROM (
  SELECT
    USER_ID, /*用户编号*/
    AMT      /*订单金额*/
  FROM (
    SELECT
      USER_ID,
      SUM(AMT) AMT
    FROM ORDER_INFO O
    WHERE O.CREATE_TIME >= :begin AND O.CREATE_TIME < :end
  ) T
  ORDER BY AMT DESC
  LIMIT 100
) SQLTOOL
```

sql-paging没有误杀无辜者，不该去掉的当然要保留原样，这时候仅仅做了必要的包装。

### pageSql

用于根据实际查询的SQL生成分页查询SQL，它也不是简单得对源SQL包裹子查询，同样是按需智能决策。继续上述三个例子：

1. 

```
SELECT
  S.STAFF_ID,
  S.STAFF_NAME,
  S.DEPARTMENT_ID,
  S.POSITION,
  S.STATUS
FROM STAFF_INFO S
ORDER BY S.STAFF_ID
```
得到的分页查询SQL（以页容量为10，页码第2页为例）：

1.1. MySQL

```
SELECT
  S.STAFF_ID,
  S.STAFF_NAME,
  S.DEPARTMENT_ID,
  S.POSITION,
  S.STATUS
FROM STAFF_INFO S
ORDER BY S.STAFF_ID
LIMIT 10,10
```

1.2. Oracle

```
SELECT
  *
FROM (
  SELECT
    SQLTOOL.*,
    ROWNUM SQLTOOL_RN 
  FROM (
    SELECT
      S.STAFF_ID,
      S.STAFF_NAME,
      S.DEPARTMENT_ID,
      S.POSITION,
      S.STATUS
    FROM STAFF_INFO S
    ORDER BY S.STAFF_ID
  ) SQLTOOL
  WHERE ROWNUM <= 20
)
WHERE SQLTOOL_RN > 10
```

1.3. PostgresSQL

```
SELECT
  S.STAFF_ID,
  S.STAFF_NAME,
  S.DEPARTMENT_ID,
  S.POSITION,
  S.STATUS
FROM STAFF_INFO S
ORDER BY S.STAFF_ID
LIMIT 10 OFFSET 10
```

2. 

```
SELECT
  USER_ID,
  SUM(AMT) AMT
FROM ORDER_INFO O
WHERE O.CREATE_TIME >= :begin AND O.CREATE_TIME < :end
GROUP BY USER_ID
ORDER BY SUM(AMT) DESC
```
得到的分页查询SQL（以页容量为10，页码第2页为例）：

2.1. MySQL：

```
SELECT
  USER_ID,
  SUM(AMT) AMT
FROM ORDER_INFO O
WHERE O.CREATE_TIME >= :begin AND O.CREATE_TIME < :end
GROUP BY USER_ID
ORDER BY SUM(AMT) DESC
LIMIT 10,10
```

2.2. Oracle

```
SELECT
  *
FROM (
  SELECT
    SQLTOOL.*,
    ROWNUM SQLTOOL_RN 
  FROM (
    SELECT
      USER_ID,
      SUM(AMT) AMT
    FROM ORDER_INFO O
    WHERE O.CREATE_TIME >= :begin AND O.CREATE_TIME < :end
    GROUP BY USER_ID
    ORDER BY SUM(AMT) DESC
  ) SQLTOOL
  WHERE ROWNUM <= 20
)
WHERE SQLTOOL_RN > 10

```

2.3. PostgresSQL

```
SELECT
  USER_ID,
  SUM(AMT) AMT
FROM ORDER_INFO O
WHERE O.CREATE_TIME >= :begin AND O.CREATE_TIME < :end
GROUP BY USER_ID
ORDER BY SUM(AMT) DESC
LIMIT 10 OFFSET 10
```

## 参与贡献

1.  Fork 本仓库
2.  新建 Feat_xxx 分支
3.  提交代码
4.  新建 Pull Request

## 相关链接

DSL开源地址：https://gitee.com/tenmg/dsl
