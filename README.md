## 通过zookeeper实现一个主从系统
### Master
#### 功能
```
1.对任务分配给worker(随机分配).
2.对宕机的worker的未完成任务进行重新分配.
3.实现高可用,master宕机后,back-master可以成为master.
```
#### 逻辑
```
1.监听/workers目录,维护workers列表.实现宕机worker的task迁移.对新增worker的任务分配.
2.监听/tasks目录,维护tasks列表.
3.监听/master,实现主备.
```
### Worker
#### 功能
```
1.执行master分配的任务,并将结果返回.
```
### Client
#### 功能
```
1.提交任务.
```
