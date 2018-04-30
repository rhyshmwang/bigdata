# 大数据管理技术

## 第二次上机作业

# 分工

| | 王映 | 王雨琦 |
| - | :-: | -: |
| 建库代码 | MongoDB，Cassandra | Redis |
| 查询代码 | 利用索引等技术进行优化 | 获取数据，在本地筛选 |
| 报告 | 报告完善 | 报告初稿 |

# 实验环境

| | Key | Value |
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

测试输出结果（简单说明）

```text
查询1 
    ...
    {u'p': u'isCitizenOf', u'o': u'Mauritius'}
    {u'p': u'isCitizenOf', u'o': u'Australia'}
    {u'p': u'isCitizenOf', u'o': u'The_Bahamas'}
    total result count is 124
查询2 
    {u'p': u'isLeaderOf', u's': u'Andranik'}
    {u'p': u'isLeaderOf', u's': u'Aghbiur_Serob'}
    {u'p': u'isLeaderOf', u's': u'Arabo'}
    {u'p': u'isLeaderOf', u's': u'Kevork_Chavush'}
    total result count is 4
查询3 
    ...
    Louis_I_of_Hungary
    Richard_Shackleton_Pope
    Scott_Myers
    Tunku_Ismail_Idris
    total result count is 108
查询4 
    the expected S is fa_, 5 cnts
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
比如 key 155 的si是Elizabeth_II，则 sadd spomillion.index.s:Elizabeth_II 155。
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
| 查询1 | 7.700000E+01 | 6.927200E-02 |
| 查询2 | 1.310000E+02 | 4.440000E-03 |
| 查询3 | 2.180000E+02 | 7.501687E+01 |
| 查询4 | 3.950000E+02 | 5.787800E+00 |

### MongoDB

| | 优化前 | 优化后 |
| - | :-: | -: |
| 查询1 | 2.560000E-04 | 2.350000E-04 |
| 查询2 | 3.790000E-04 | 2.060000E-04 |
| 查询3 | 6.102332E+03 | 1.201150E+01 |
| 查询4 | 2.824407E+01 | 2.844010E+01 |

### Cassandra

| | 优化前 | 优化后 |
| - | :-: | -: |
| 查询1 | 4.934752E+00 | 7.316100E-02 |
| 查询2 | 2.402006E+00 | 6.701000E-03 |
| 查询3 | 3.214405E+04 | 3.183573E+01 |
| 查询4 | 2.015562E+01 | 1.729505E+01 |

## 基本查询效率横向比较

运行5次取平均值，结果见后文


## 优化后的查询效率横向比较

同样地运行5次取平均值，结果见后文

# 存储模式和优化解释

统计时间采用python的`datetime`库，使用了`timedelta.totalseconds()`方法

### Redis

* 基本存储方式

```python
r = redis.Redis(host='127.0.0.1', port=6379,db=0)
regex = r'(.*?)\s(.*?)\s(.*?)\s'
rePattern = re.compile(regex)
i = 0
for line in tqdm(lines):
    i = i + 1
    matchObj = rePattern.match(line)
    r.hset('spomillion:' + str(i), 's', matchObj.group(1))
    r.hset('spomillion:' + str(i), 'p', matchObj.group(2))
    r.hset('spomillion:' + str(i), 'o', matchObj.group(3))
```

建库时间：92s

* 优化

以下只展示如何在 s 上建立索引的**关键代码**

```python
# 首先读出所有数据，存在 rs1 中，比如第i条的s是si
si = rs1[i]['s']
# 把对应的行加到si 的索引里
index_s_key_str = 'spomillion.index.s:'
tmp_index_s_key_str = index_s_key_str + si
tmp_key_str = key_str + str(cur_row)
pipe.sadd(tmp_index_s_key_str, tmp_key_str)
```

即如前面所说，key 155 的si是Elizabeth_II，则 `sadd spomillion.index.s:Elizabeth_II 155`。

建立s索引：5.742165E+02s

建立o索引：5.221601E+02s

没有对P建立索引。

### MongoDB

* 基本存储方式

```python
rows = []
spo = line.strip().split(' ', 3)
row_dict = {'s': spo[0],
            'p': spo[1],
            'o': spo[2]}
rows.append(row_dict)
# 将所有数据按字典形式存在了 rows中

client = MongoClient('localhost', 27017)
db = client.get_database('bigdata')
spo = db.get_collection('spomillion')

rs = spo.insert_many(rows)
```

建库时间：39s

* 优化

利用MongoDB自带的功能，在s,p,o上建立索引，如下

```python
def index_s():
    spo.ensure_index([('s', pymongo.ASCENDING)])
    return
```

建立s索引：4.665801E+00 s

建立p索引：2.437039E+00 s

建立o索引：4.221476E+00 s

### Cassandra

* 基本存储方式

```python
create_spo_cql = 'create table if not exists spomillion' \
                 '("id" int, "s" varchar, "p" varchar, "o" varchar, primary key("id", "s") );'
session.execute(create_spo_cql)

# 用unlogged batch 每次插入100行
insert_spo_cql = 'insert into bigdata.spomillion("id", "s", "p", "o") values ' \
                 '({0}, \'{1}\', \'{2}\', \'{3}\');'
batch_cql_head = 'begin unlogged batch \n'
batch_cql_tail = '\napply batch;'

for i in tqdm.trange(0, len(rows), 100):
    if (len(rows) - i) <= 100:
        # 退出循环，单独处理
        last_batch_begin = i  # 从最后的一个 *00 开始
        break

    batch_cql = batch_cql_head
    for j in range(i, i + 100):
        spo = rows[j].split(' ', 3)
        batch_cql = batch_cql + insert_spo_cql.format(j+1 , spo[0], spo[1], spo[2])
    batch_cql = batch_cql + batch_cql_tail
    session.execute(batch_cql)
for i in tqdm.trange(last_batch_begin, len(rows)):
    spo = rows[i].split(' ', 3)
    session.execute(insert_spo_cql.format(i+1, spo[0], spo[1], spo[2]))
```

建库时间：64s

* 优化

利用Cassandra自带的功能，在s,p,o上建立索引，如下

```python
def index_s():
    session.execute("create index if not exists on bigdata.spomillion(\"s\")")
    return
```

建立s索引：4.691090E-01 s

建立p索引：1.505090E-01 s

建立o索引：1.743410E-01 s

# 4个查询的构建方法

### Redis

##### 本地筛选

* 问题1

```python
# 挨个按key取出去比较 si
for i in range(1, maxSize + 1):
    valS = r.hget("spomillion:"+str(i), 's')
    if valS == si:
        flag += 1
```

* 问题2

同上，挨个比较 oi

* 问题3

```python
sHavingP = {}
# 对S按位做标记，如果最后都标记到了,就是所求的
for i in range(1, maxSize + 1):
    valP = r.hget("spomillion:"+str(i), 'p')
    valS = r.hget("spomillion:"+str(i), 's')
    print "<S, P>", "<", valP, ",", valS, ">"
    if valP == p1:
        if sHavingP.has_key(valS) :
            sHavingP[valS] |= 1
        else:
            sHavingP[valS] = 1
    elif valP == p2:
        if sHavingP.has_key(valS):
            sHavingP[valS] |= 2
        else:
            sHavingP[valS] = 2
for key, value in sHavingP.items():
    if value == 3:
        ++ flag
        print flag, "possible S: ", key
```

* 问题4

```python
    利用一个字典计数，全表扫描
    sHavingO = {}
    for i in range(1, maxSize + 1):
        print "i: ", i
        valS = r.hget("spomillion" + str(i), 's')
        valO = r.hget("spomillion" + str(i), 'o')
        if valO == oi:
            if sHavingO.has_key(valS):
                sHavingO[valS] += 1
            else:
                sHavingO = 1
    result = 0
    for key, value in sHavingO.items():
        if value > result:
            result = value
```

###### 优化

* 问题1

利用在s上建立的索引，直接获取s等于si的hash的key，从而可以直接查key。

```python
# 在s上建立了索引之后
rs1 = r.smembers('spomillion.index.s:' + si)
rs2 = []
for key_str in rs1:
    p = r.hget(key_str, 'p')
    o = r.hget(key_str, 'o')
    rs2.append({'p': p, 'o': o})
return rs2
```

* 问题2

和问题一一样的做法，不过用的是o的索引。

* 问题3

P不适合做索引查询，所以做两次查询，第一次查出所有拥有p1的S。

关键代码如下

```python
first_step = set()
    p = rs1[i]['p']
    s = rs1[i]['s']
    if p == p1:
        first_step.add(s) # 第一次查到的s存进来
```

第二次根据得到的s集合，利用s的索引，查询并保留拥有p2的S

```python
for s in first_step:
    # 根据第一步得到的s，以及s的索引，找到对应的项
    rs2 = r.smembers('spomillion.index.s:' + s)
    for row_key in rs2:
        pipe.hgetall(row_key)
    rs3 = pipe.execute()
    # 看看是否有 p2
    for row_dict in rs3:
        if row_dict['p'] == p2:
            sec_step.add(s)
return sec_step
```


* 问题4

首先利用o的索引获取拥有oi的s

然后在本地对s计数，扫描并选择出现最多的s。

关键代码如下：

```python
rs1 = r.smembers('spomillion.index.o:' + oi)
rs2 = {}
for key_str in rs1:
    s = r.hget(key_str, 's')
    if s not in rs2:
        rs2[s] = 1
    else:
        rs2[s] += 1
max_cnt = 0
max_s = None
# 扫描一遍计数字典，得出出现最多的S
for the_key in rs2.keys():
    if max_cnt < rs2[the_key]:
        max_cnt = rs2[the_key]
        max_s = the_key
return max_s, max_cnt
```

### MongoDB

与Redis相比，提供了良好易用的API。因此优化只体现在建立索引上，构建查询的方法不变。

* 问题1

```python
res1 = spo.find({'s': si}, {'p':1, 'o': 1, '_id': 0})
return res1
```

* 问题2

```python
res1 = spo.find({'o': oi}, {'s':1, 'p': 1, '_id': 0})
return res1
```

* 问题3

同样地，先p1，再在有p1的s里，找出有p2的

```python
res1 = spo.find({'p': p1}).distinct('s')
# 查这些s，筛选出有p2的
sec_step = []
for s in res1:
    tmp_res = spo.find({'s': s, 'p': p2})
    if tmp_res.count() != 0:
        sec_step.append(s)
return sec_step
```

* 问题4

```python
# 首先获得拥有这样 oi 的S
res1 = spo.find({'o': oi}, {'s': 1, '_id': 0})

# 然后计数并扫描，与Redis的优化版实现类似，这里不再重复
```

### Cassandra

都是利用CQL语句，因此非优化版的区别只在于所有查询语句末尾多了 ALLOW FILTERING;

* 问题1

```python
res = session.execute("select p, o from spomillion where s = '{0}'".format(si))
return res.current_rows
```

* 问题2

```python
res = session.execute("select s, p from spomillion where o = '{0}'".format(oi))
return res.current_rows
```

* 问题3

```python
# 先找出所有拥有 p1 的 S，再找出所有拥有 p2 的 S
rs1 = session.execute("select s from spomillion where p = '{0}'".format(p1))
# 选出S的集合
first_step = set()
for row in rs1:
    first_step.add(row.s)
# 然后查出所有拥有 p2 的 S
sec_step = set()
for s in first_step:
    tmp_rs = session.execute("select s, p from spomillion where s = '{0}'".format(s))
    for row in tmp_rs:
        if row.p == p2:
            sec_step.add(s)
            break # 既然有了，就不需要继续遍历这个s的结果
return sec_step
```

* 问题4

```python
# 首先获得拥有这样 oi 的 S
rs1 = session.execute("select s from spomillion where o = '{0}'".format(oi))

# 然后计数并扫描，与Redis的优化版实现类似，这里不再重复
```

# 基本查询效率横向比较

单位 秒

| | Redis | MongoDB | Cassandra |
| - | :-: | :-: | -: |
| 查询1 | 7.700000E+01 | 2.560000E-04 | 4.934752E+00 |
| 查询2 | 1.310000E+02 | 3.790000E-04 | 2.402006E+00 |
| 查询3 | 2.180000E+02 | 6.102332E+03 | 3.214405E+04 |
| 查询4 | 3.950000E+02 | 2.824407E+01 | 2.015562E+01 |

# 优化后查询效率横向比较

单位 秒

| | Redis | MongoDB | Cassandra |
| - | :-: | :-: | -: |
| 查询1 | 6.927200E-02 | 2.350000E-04 | 7.316100E-02 |
| 查询2 | 4.440000E-03 | 2.060000E-04 | 6.701000E-03 |
| 查询3 | 7.501687E+01 | 1.201150E+01 | 3.183573E+01 |
| 查询4 | 5.787800E+00 | 2.844010E+01 | 1.729505E+01 |

# 结论与体会

我们对于四个问题的sql表示如下：

```sql
select p, o from spo where s = si;
select s, p from spo where o = oi;
select s from spo where p = p1 and s in (select s from spo where p = p2);
select s from spo where o = oi group by s having count(*) >= all 
(select count(*) from spo where o = oi group by s); 
```

相比于关系型数据库所提供的丰富的查询功能：子查询、聚集等，nosql在查询功能上的支持要弱许多。MongoDB和Cassandra提供了简单的索引支持，而Redis需要自己利用数据结构维护索引。为了实现这4个查询，有大量工作是在客户端做的，比如问题三中的分两次查询、问题四中的计数找最大（MongoDB也提供了聚集功能，不过我们没有使用）。

实现难度上，Redis最为复杂，需要自己维护索引，且如果不维护索引，无法实现非key查询。Cassandra和MongoDB的查询实现难度相当，主要是问题3的子查询变为2次查询，问题4的本地计数。

执行效率上，不使用索引优化时。

Redis只能全部取出（或分片取出）并挨个检查，效率最低。

MongoDB提供了查询API，可以直接使用，查询单个字段非常快，但是组合查询较慢。

Cassandra需要进行全表扫描并过滤，单个字段查询效率居中，组合查询最慢。

使用索引优化时

Redis利用set结构做key的索引，实现了非key查询，效率提升3、4个数量级。

MongoDB的效率并没有明显变化，不过组合查询的速度提升了2个数量级。

Cassandra不再需要全表扫描并过滤，效率提升了2、3个数量级，组合查询速度提升了2、3个数量级。

加了索引之后三个数据库之间的差距不再明显，按查询来看，查询1、2、3MongoDB最优，4Redis最优。
