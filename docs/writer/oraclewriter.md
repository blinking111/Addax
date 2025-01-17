# Oracle Writer

OracleWriter 插件实现了写入数据到 Oracle 目的表的功能。

## 配置样例

这里使用一份从内存产生到 Oracle 导入的数据。

=== "job/stream2oracle.json"

  ```json
  --8<-- "jobs/oraclewriter.json"
  ```

## 参数说明

OracleWriter 基于 [rdbmswriter](../rdbmswriter) 实现，因此可以参考 rdbmswriter 的所有配置项，并在此基础上增加了一些 OracleWriter 特有的配置项。

| 配置项    | 是否必须 | 默认值 | 描述                                     |
| :-------- | :------: | ------ | ---------------------------------------- |
| writeMode |    否    | insert | 写入方式，支持 insert， update，详见下文 |

[1]: http://www.oracle.com/technetwork/database/enterprise-edition/documentation/index.html

### writeMode

默认情况下， 采取 `insert into` 语法写入 Oracle 表，如果你希望采取主键存在时更新，不存在则写入的方式，也就是 Oracle 的 `merge into` 语法， 可以使用 `update` 模式。假定表的主键为 `id` ,则 `writeMode` 配置方法如下：

```json
"writeMode": "update(id)"
```

如果是联合唯一索引，则配置方法如下：

```json
"writeMode": "update(col1, col2)"
```

注： `update` 模式在 `3.1.6` 版本首次增加，之前版本并不支持。


