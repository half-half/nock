![Nock](https://github.com/half-half/Content/blob/master/nock/logo.jpg)

# nock是什么
nock是一款依赖于数据库的分布式调度引擎， 基于著名的开源调度框架[Quartz](https://github.com/quartz-scheduler/quartz)所做的优化， 
作为Quartz的扩展包，Quartz用到的地方均可以的使用Nock来替代。比起Quartz， Nock提供了分布式环境下调度引擎的可扩展性，更高的吞吐量。

# why nock
* 兼容Quartz，项目接入方法与Quartz一样，没有学习成本
* 对Quartz的改进采用插件式设计，没有侵入性的，与Quartz一起升级
* 集群模式下去掉Quartz对数据库悲观锁的依赖，采用无锁设计，大幅度提供调度的吞吐量或者准确度
* 支持集群模式下的横向扩展能力
* 已经在大规模生产环境落地

# 快速体验
[开始](https://github.com/half-half/nock/wiki/1%E3%80%81%E5%BF%AB%E9%80%9F%E5%BC%80%E5%A7%8B)

# nock文档
[Document](https://github.com/half-half/nock/wiki)

# 开发团队
[关于我们]()






