# sql-paging

## 介绍
这是一个SQL分页查询方言类库。通过调用相关API，可快速将一个普通SQL转换为一个特定数据库的计数（`COUNT`）SQL或分页查询SQL。

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
## 数据库支持

数据库支持及内置方言如下

数据库     | 支持版本 |     方言实现类
-----------|---------|-------------------------
Mysql      | 1.0+    | MySQLPagingDialect
Oracle     | 1.0+    | OraclePagingDialect
PostgreSQL | 1.0+    | PostgreSQLPagingDialect
SQLServer  | 1.0+    | SQLServerPagingDialect

## 参与贡献

1.  Fork 本仓库
2.  新建 Feat_xxx 分支
3.  提交代码
4.  新建 Pull Request


## 相关链接

DSL开源地址：https://gitee.com/tenmg/dsl
