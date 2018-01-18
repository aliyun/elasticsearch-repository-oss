# elasticsearch-repository-oss


要备份你的集群，你可以使用 `snapshot` API。这个会拿到你集群里当前的状态和数据然后保存到一个共享仓库里。这个备份过程是"智能"的。你的第一个快照会是一个数据的完整拷贝，但是所有后续的快照会保留的是已存快照和新数据之间的差异。随着你不时的对数据进行快照，备份也在增量的添加和删除。这意味着后续备份会相当快速，因为它们只传输很小的数据量。

* [OSS快照迁移文档](https://github.com/zhichen/elasticsearch-repository-oss/wiki/OSS快照迁移)


## 创建仓库
```
PUT _snapshot/my_backup 
{
    "type": "oss",
    "settings": {
        "endpoint": "http://oss-cn-hangzhou-internal.aliyuncs.com", <1>
        "access_key_id": "xxxx", 
        "secret_access_key": "xxxxxx", 
        "bucket": "xxxxxx", <2>
        "compress": true
    }
}
```
* <1> 本处的OSS, 要求和你的elasticsearch集群在同一个region中, 这里的endpoint填的是这个region对应的内网地址 ,具体参考 https://help.aliyun.com/document_detail/31837.html?spm=5176.doc31922.6.577.YxqZYt 中`ECS访问的内网Endpoint`一栏
* <2> 需要一个已经存在的OSS bucket




假设我们上传的数据非常大, 我们可以限制snapshot过程中分块的大小,超过这个大小，数据将会被分块上传到OSS中

```
POST _snapshot/my_backup/ <1>
{
    "type": "oss",
    "settings": {
        "endpoint": "http://oss-cn-hangzhou-internal.aliyuncs.com", 
        "access_key_id": "xxxx", 
        "secret_access_key": "xxxxxx", 
        "bucket": "xxxxxx", 
        "chunk_size": "500mb",
        "base_path": "snapshot/" <2>
    }
}
```
* <1> 注意我们用的是 `POST` 而不是 `PUT` 。这会更新已有仓库的设置。
* <2> base_path 设置仓库的起始位置默认为根目录

## 列出仓库信息
```
GET _snapshot
```
* 也可以使用 `GET _snapshot/my_backup` 获取指定仓库的信息

### 备份快照迁移
如果需要将快照迁移到另一个集群.只需要备份到OSS, 然后再在新的集群上注册一个快照仓库(相同的OSS),设置`base_path`的位置为备份文件所在的地方，然后执行恢复备份的命令即可。

### 快照所有打开的索引 （以下内容和官方一致）

一个仓库可以包含多个快照。每个快照跟一系列索引相关（比如所有索引，一部分索引，或者单个索引）。当创建快照的时候，你指定你感兴趣的索引然后给快照取一个唯一的名字。

让我们从最基础的快照命令开始：

```
PUT _snapshot/my_backup/snapshot_1
```

这个会备份所有打开的索引到 `my_backup` 仓库下一个命名为 `snapshot_1` 的快照里。这个调用会立刻返回，然后快照会在后台运行。



通常你会希望你的快照作为后台进程运行，不过有时候你会希望在你的脚本中一直等待到完成。这可以通过添加一个 `wait_for_completion` 标记实现：

```
PUT _snapshot/my_backup/snapshot_1?wait_for_completion=true
```

这个会阻塞调用直到快照完成。注意大型快照会花很长时间才返回。




### 快照指定索引

默认行为是备份所有打开的索引。不过如果你在用 Kibana，你不是真的想要把所有诊断相关的 `.kibana` 索引也备份起来。可能你就压根没那么大空间备份所有数据。

这种情况下，你可以在快照你的集群的时候指定备份哪些索引：

```
PUT _snapshot/my_backup/snapshot_2
{
    "indices": "index_1,index_2"
}
```

这个快照命令现在只会备份 `index1` 和 `index2` 了。

### 列出快照相关的信息

一旦你开始在你的仓库里积攒起快照了，你可能就慢慢忘记里面各自的细节了——特别是快照按照时间划分命名的时候（比如， `backup_2014_10_28` ）。

要获得单个快照的信息，直接对仓库和快照名发起一个 `GET` 请求：

```
GET _snapshot/my_backup/snapshot_2
```

这个会返回一个小响应，包括快照相关的各种信息：

```
{
   "snapshots": [
      {
         "snapshot": "snapshot_1",
         "indices": [
            ".marvel_2014_28_10",
            "index1",
            "index2"
         ],
         "state": "SUCCESS",
         "start_time": "2014-09-02T13:01:43.115Z",
         "start_time_in_millis": 1409662903115,
         "end_time": "2014-09-02T13:01:43.439Z",
         "end_time_in_millis": 1409662903439,
         "duration_in_millis": 324,
         "failures": [],
         "shards": {
            "total": 10,
            "failed": 0,
            "successful": 10
         }
      }
   ]
}
```

要获取一个仓库中所有快照的完整列表，使用 `_all` 占位符替换掉具体的快照名称：

```
GET _snapshot/my_backup/_all
```

### 删除快照

最后，我们需要一个命令来删除所有不再有用的旧快照。这只要对仓库/快照名称发一个简单的 `DELETE` HTTP 调用：

```
DELETE _snapshot/my_backup/snapshot_2
```

用 API 删除快照很重要，而不能用其他机制（比如手动删除）。因为快照是增量的，有可能很多快照依赖于过去的段。`delete` API 知道哪些数据还在被更多近期快照使用，然后会只删除不再被使用的段。

但是，如果你做了一次人工文件删除，你将会面临备份严重损坏的风险，因为你在删除的是可能还在使用中的数据。


### 监控快照进度

`wait_for_completion` 标记提供了一个监控的基础形式，但哪怕只是对一个中等规模的集群做快照恢复的时候，它都真的不够用。

另外两个 API 会给你有关快照状态更详细的信息。首先你可以给快照 ID 执行一个 `GET`，就像我们之前获取一个特定快照的信息时做的那样：

```
GET _snapshot/my_backup/snapshot_3
```

如果你调用这个命令的时候快照还在进行中，你会看到它什么时候开始，运行了多久等等信息。不过要注意，这个 API 用的是快照机制相同的线程池。如果你在快照非常大的分片，状态更新的间隔会很大，因为 API 在竞争相同的线程池资源。

更好的方案是拽取 `_status` API 数据：

```
GET _snapshot/my_backup/snapshot_3/_status
```

`_status` API 立刻返回，然后给出详细的多的统计值输出：

```
{
   "snapshots": [
      {
         "snapshot": "snapshot_3",
         "repository": "my_backup",
         "state": "IN_PROGRESS", <1>
         "shards_stats": {
            "initializing": 0,
            "started": 1, <2>
            "finalizing": 0,
            "done": 4,
            "failed": 0,
            "total": 5
         },
         "stats": {
            "number_of_files": 5,
            "processed_files": 5,
            "total_size_in_bytes": 1792,
            "processed_size_in_bytes": 1792,
            "start_time_in_millis": 1409663054859,
            "time_in_millis": 64
         },
         "indices": {
            "index_3": {
               "shards_stats": {
                  "initializing": 0,
                  "started": 0,
                  "finalizing": 0,
                  "done": 5,
                  "failed": 0,
                  "total": 5
               },
               "stats": {
                  "number_of_files": 5,
                  "processed_files": 5,
                  "total_size_in_bytes": 1792,
                  "processed_size_in_bytes": 1792,
                  "start_time_in_millis": 1409663054859,
                  "time_in_millis": 64
               },
               "shards": {
                  "0": {
                     "stage": "DONE",
                     "stats": {
                        "number_of_files": 1,
                        "processed_files": 1,
                        "total_size_in_bytes": 514,
                        "processed_size_in_bytes": 514,
                        "start_time_in_millis": 1409663054862,
                        "time_in_millis": 22
                     }
                  },
                  ...
```
* <1> 一个正在运行的快照会显示 `IN_PROGRESS` 作为状态。
* <2> 这个特定快照有一个分片还在传输（另外四个已经完成）。

响应包括快照的总体状况，但也包括下钻到每个索引和每个分片的统计值。这个给你展示了有关快照进展的非常详细的视图。分片可以在不同的完成状态：

`INITIALIZING`::
    分片在检查集群状态看看自己是否可以被快照。这个一般是非常快的。

`STARTED`::
    数据正在被传输到仓库。
    
`FINALIZING`::
    数据传输完成；分片现在在发送快照元数据。
    
`DONE`::
    快照完成！
    
`FAILED`::
    快照处理的时候碰到了错误，这个分片/索引/快照不可能完成了。检查你的日志获取更多信息。


### 取消一个快照

最后，你可能想取消一个快照或恢复。因为它们是长期运行的进程，执行操作的时候一个笔误或者过错就会花很长时间来解决——而且同时还会耗尽有价值的资源。

要取消一个快照，在他进行中的时候简单的删除快照就可以：

```
DELETE _snapshot/my_backup/snapshot_3
```

这个会中断快照进程。然后删除仓库里进行到一半的快照。


### 从快照恢复

一旦你备份过了数据，恢复它就简单了：只要在你希望恢复回集群的快照 ID 后面加上 `_restore` 即可：

```
POST _snapshot/my_backup/snapshot_1/_restore
```

默认行为是把这个快照里存有的所有索引都恢复。如果 `snapshot_1` 包括五个索引，这五个都会被恢复到我们集群里。和 `snapshot` API 一样，我们也可以选择希望恢复具体哪个索引。

还有附加的选项用来重命名索引。这个选项允许你通过模式匹配索引名称，然后通过恢复进程提供一个新名称。如果你想在不替换现有数据的前提下，恢复老数据来验证内容，或者做其他处理，这个选项很有用。让我们从快照里恢复单个索引并提供一个替换的名称：

```
POST /_snapshot/my_backup/snapshot_1/_restore
{
    "indices": "index_1", <1>
    "rename_pattern": "index_(.+)", <2>
    "rename_replacement": "restored_index_$1" <3>
}
```
* <1> 只恢复 `index_1` 索引，忽略快照中存在的其余索引。
* <2> 查找所提供的模式能匹配上的正在恢复的索引。
* <3> 然后把它们重命名成替代的模式。

这个会恢复 `index_1` 到你及群里，但是重命名成了 `restored_index_1` 。



和快照类似， `restore` 命令也会立刻返回，恢复进程会在后台进行。如果你更希望你的 HTTP 调用阻塞直到恢复完成，添加 `wait_for_completion` 标记：

```
POST _snapshot/my_backup/snapshot_1/_restore?wait_for_completion=true
```




### 监控恢复操作

从仓库恢复数据借鉴了 Elasticsearch 里已有的现行恢复机制。在内部实现上，从仓库恢复分片和从另一个节点恢复是等价的。

如果你想监控恢复的进度，你可以使用 `recovery` API。这是一个通用目的的 API，用来展示你集群中移动着的分片状态。

这个 API 可以为你在恢复的指定索引单独调用：

```
GET restored_index_3/_recovery
```

或者查看你集群里所有索引，可能包括跟你的恢复进程无关的其他分片移动：

```
GET /_recovery/
```

输出会跟这个类似（注意，根据你集群的活跃度，输出可能会变得非常啰嗦！）：

```
{
  "restored_index_3" : {
    "shards" : [ {
      "id" : 0,
      "type" : "snapshot", <1>
      "stage" : "index",
      "primary" : true,
      "start_time" : "2014-02-24T12:15:59.716",
      "stop_time" : 0,
      "total_time_in_millis" : 175576,
      "source" : { <2>
        "repository" : "my_backup",
        "snapshot" : "snapshot_3",
        "index" : "restored_index_3"
      },
      "target" : {
        "id" : "ryqJ5lO5S4-lSFbGntkEkg",
        "hostname" : "my.fqdn",
        "ip" : "10.0.1.7",
        "name" : "my_es_node"
      },
      "index" : {
        "files" : {
          "total" : 73,
          "reused" : 0,
          "recovered" : 69,
          "percent" : "94.5%" <3>
        },
        "bytes" : {
          "total" : 79063092,
          "reused" : 0,
          "recovered" : 68891939,
          "percent" : "87.1%"
        },
        "total_time_in_millis" : 0
      },
      "translog" : {
        "recovered" : 0,
        "total_time_in_millis" : 0
      },
      "start" : {
        "check_index_time" : 0,
        "total_time_in_millis" : 0
      }
    } ]
  }
}
```
* <1> `type` 字段告诉你恢复的本质；这个分片是在从一个快照恢复。
* <2> `source` 哈希描述了作为恢复来源的特定快照和仓库。
* <3> `percent` 字段让你对恢复的状态有个概念。这个特定分片目前已经恢复了 94% 的文件；它就快完成了。

输出会列出所有目前正在经历恢复的索引，然后列出这些索引里的所有分片。每个分片里会有启动/停止时间、持续时间、恢复百分比、传输字节数等统计值。

### 取消一个恢复

要取消一个恢复，你需要删除正在恢复的索引。因为恢复进程其实就是分片恢复，发送一个 `删除索引` API 修改集群状态，就可以停止恢复进程。比如：

```
DELETE /restored_index_3
```

如果 `restored_index_3` 正在恢复中，这个删除命令会停止恢复，同时删除所有已经恢复到集群里的数据。






