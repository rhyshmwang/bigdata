# 概述

## 问题

```text
4个查询
数据形式为三元组<S, P, O>:
1:给定一个si，给出它所有的P和O，<si, P, O>
2:给定一个oi, 给出它所有的S和P，<S, P, oi>
3:给定两个p1,p2, 给出同时拥有它们的S，<S, p1, *>, <S, p2, *>
4:给定一个oi, 给出拥有这样oi最多的S
```

测试输入如下

```text
查询1 si = Elizabeth_II 
查询2 oi = Armenian_fedayi 
查询3 p1 = isLeaderOf p2 = owns
查询4 oi = India
```

## 使用数据

yagoThreeSimplified.txt。共12430689行，其中三行数据不是三元组，有效数据12430686行。

我们使用了其中的**前100万**行，有效数据**100万行**。

## 存储模式和优化

### Redis

* 基本存储模式：
```text
1000000个key-value对，key是spomillion:*，对应的value是一个含"s""p""o"3个field的hash类型
```

* 优化：
```text
针对不同的查询分别建立索引提高查询性能：查询1、3建立关于"s"的索引，查询2、4建立关于"o"的索引
索引的构建采用了Redis的set数据类型，每个对应的value生成一个set，set内部保存着这个value所在的hash的key值。
比如 key 155 的si是Elizabeth_II，则 sadd spo.index.s:Elizabeth_II 155。
```

### MongoDB

* 基本存储模式：
```text
一个集合中，存储了1000000个文档。每个文档有3个字段（不包括自动生成的'_id'）['s', 'p', 'o']。
```

* 优化：
```text
针对不同的查询分别建立索引提高查询性能：利用MongoDB自带的索引功能。
对 's' 'p' 'o'字段都建立了索引。
```

### Cassandra

* 基本存储模式：
```text
每个表有4列，[id, s, p, o]，id 作为主键，s作为第二主键。
表中共有1000000行。
```

* 优化：
```text
利用Cassandra自带的索引功能。
在 's' 'p' 'o' 上都建立索引。
```

## 查询效率纵向比较

单位 秒

### Redis

| | 优化前 | 优化后 |
| - | :-: | -: |
| 查询1 | | |
| 查询2 | | |
| 查询3 | | |
| 查询4 | | |

### MongoDB

| | 优化前 | 优化后 |
| - | :-: | -: |
| 查询1 | | |
| 查询2 | | |
| 查询3 | | |
| 查询4 | | |

### Cassandra

| | 优化前 | 优化后 |
| - | :-: | -: |
| 查询1 | | |
| 查询2 | | |
| 查询3 | | |
| 查询4 | | |

## 基本查询效率横向比较
```text
运行5次取平均值，结果见后文
```

## 优化后的查询效率横向比较
```text
同样地运行5次取平均值，结果见后文
```

# 分工

| | 王映 | 王雨琦 |
| - | :-: | -: |
| 建库代码 | MongoDB，Cassandra | Redis |
| 查询代码 | 利用索引等技术进行优化 | 获取数据，在本地筛选 |
| 报告 | 报告完善 | 报告初稿 |

# 实验环境

| | |  |
| - | :-: | -: |
| 运行环境 | CPU | 2.9ghz core i5 |
| | 内存 | 8g 1867mhz ddr3 |
| | OS | macOS10.13.3 |
| | IDE | PyCharm2017.4(python2.7) |
| 数据库版本 | Cassandra | 3.11.2_1 |
| | MongoDB | 3.6.3 |
| | Redis | 4.0.9 |
| | redis-py | 2.10.6 |
| | pymongo | 3.6.1 |
| | cassandra-driver | 3.14.0 |





