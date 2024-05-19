![Building a Log-Structured Merge Tree Database - Part 1](https://tmsvr.com/content/images/2023/07/cover.png)

## LSM-Tree 

  这是一款采用**基于时间戳的并发控制来支持事务的LSM-Tree存储引擎实现,** 该项目旨在满足我对LSM-Tree论文的复现以优化, 结合我以前学过的一些知识, 意在构建一个“麻雀虽小,五脏俱全”的存储系统。包含基本的存储引擎功能,CRUD接口,实现常见的优化,对事务以及并发的支持。

  本项目广泛吸收和借鉴一些著名的开源项目实现, 其中LevelDB, RocksDB, BadgerDB对我实现该系统影响最为深刻。此外, 特别感谢迟先生开源的LSM-Tree书籍、Rust中文社区的Mike Tang老师以及PingCAP的Talent计划, 给予我许多领域知识、工程细节与语言使用上的帮助。

## 单机架构

![img](https://miro.medium.com/v2/resize:fit:945/1*34l_7nzmhH31wgsdhTOeoQ.jpeg)

### Components: 

1. **MemTable**: 在内存中, 用于缓存新写入的数据。当MemTable达到一定容量时会触发Frozen操作,成为Immutable MemTable。

2. **Immutable MemTable**: 只读的MemTable, 在一定条件下被Flush刷到L0-SSTable上。由开机时启动的后台线程Flush_thread完成。

3. **SSTable**: LSM-Tree的核心数据结构, 最早来源于BigTable。是在磁盘上把海量数据按Key排序的的存储实体。可以进一步细分为

   1. **L0 SSTable**: 由不可变MemTable通过Flush到L0层, 这个过程也叫minor-compaction, key可能会重叠。
   2. **L1 -> Ln SSTable**：由L0层的SSTable通过Compaction(有不同的策略)压缩到更底层, key不会重叠, 这样提高查找效率。

   LSM-Tree采用追加式更新,会有数据冗余,
   **这会导致有不同程度的写/读/空间放大的问题。而不同Compact策略就是在其中作Trade-Off。**

   本项目采用RocksDB的Leveled compaction策略, 外加WaterMark用来记录当前用户正在使用的最早事务以实现GC。

5. **WAL:** 预写式日志, 用于暂存想要写入内存的数据, 如果写入内存时Crash, 则由WAL恢复。

6. **Bloom Filter:** 布隆过滤器, 用来快速判断某Key是否存在于存储引擎中, **如果不存在**可以立刻返回, 避免无效查找。

### Read Path: (eg:查找一对Key-value pair)

1. 先询问Bloom Fitler, Key是否存在存储系统中? 如果不存在则返回。
2. 如果存在, 则在内存中从新到旧遍历所有MemTable查找key是否存在。
3. 如果key在内存中没找到, 则自顶向下查找SSTs, 查找key是否存在。

   注: 
   ①因为布隆过滤器会有一定的误判概率, 所以最后可能遍历完了之后也不存在, 这是一个潜在的优化点。

   ②除了单点Lookup,本项目支持Range查询, 有`scan()`接口, 可以查询一组连续范围中的key。

### Write Path:(eg:写入一对key-value pair)

1. 先将Kv键值对写入WAL, 这样就可以在写入内存时崩溃后恢复(Recovery from Crash)。
2. 将Kv键值对写入MemTable, 注意这里的Mutable MemTable整个系统只有一张。
3. 后台线程会定期轮询, 是否当前MemTable容量满, 如果是则触发Frozen以及Flush。
4. 后台线程会定期进行Compact, 将处在上层的SSTable不断往下压, 用来保持LSM-Tree的形态。

## 设计(to be continued..)

### 第一部分：提供基本CURD功能的存储引擎

参考LevelDB的基本架构以及编码方式, 尤其是迭代器系统与Block Encoding。

### 第二部分：优化存储引擎: Compaction, Batch, Bloom Filter,  WAL ...

参考RocksDB的Leveled compaction。

### 第三部分：基于时间戳的并发版本控制

参考BadgerDB实现SSI。

