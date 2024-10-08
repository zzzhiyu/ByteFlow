# DataFlow
## 一.简介
dataflow是用python编写的一个数据集成工具，集成datax,dolphinscheduler具有如下功能:
- 自动在目标库建表
- 自动提交数据同步调度任务到dolphinscheuler(能够同步全量历史数据,增量T+1数据)
- 动态识别数据源的变更
- 数据源读取插件易扩展
## 二.依赖环境
datax,dolphinscheduler,pydolphinscheduler
## 三.流程图
 ![image](image/流程图.png)
## 四.项目架构
      ByteFlow
          |
          |----client  生成拉取任务代码源文件
          |     |----conf 配置参数
          |     |----datax 生成datax json配置任务
          |     |----dolphinscheduler 生成dolphinscheduler工作流,并提交到dolphinscheduler
          |     |----ds 元数据库操作
          |     |----model 数据模型
          |     |----reader 获取拉取源数据库的表属性信息
          |     |----util 工具包
          |     |----writer 生成目的数据库表信息,并在目标库建立合适的表
          |     |----exec_console.py 启动client的代码文件
          |
          |----service 部署在dolphinscheduler上的脚本
          |     |----conf 配置
          |     |----ds 元数据库操作
          |     |----exec_service 启动service的代码文件, 动态获取数据源的连接信息并同步数据到目的库
          |
          |----start_client.sh 启动client的shell脚本
## 五.操作流程
- 将client和service中conf目录下的配置文件配置好
- 将service的代码放在dolphinscheduler资源中心的byte_flow目录(需要先创建)下
- 在dolphinscheduler的project中创建对应的项目来存放workflow,在资源中心创建对应的目录来存放datax的配置文件
- dolphinscheduler的服务器要安装pydolphinscheduler环境
- ./start_client.py 启动客户端
## 六.使用实例
    1.请输入需要采集游戏的source_name：
    2.请输入源库db_type类型：
    3.输入源库表名:
    在库中对对应的表进行配置后，可以自动生成下面配置
    4.是否建立分区表[y/n]：
    5.表名为 {source_name}_{db_type}_{原表名称}_{ri/rf/di/df},是否修改[y/n]:
    6.选择分区列或者更新列:
    7.选择key列:
    8.是否进行布隆过滤[y/n],选择布隆列
    9.输入分区分桶数量
    10.每日执行时间
    11.此次是否进行全量拉取
## 七.待改进的地方
- 任务配置的方式是在服务器终端配置(1.终端输入命令比网页一个一个点快[狗头], 2.做前后端会增加代码的复杂度，影响阅读)
## 八.作者想说的
这段代码比较简单，不能直接用于生产环境，只是提供一个思路：数据集成和调度工具已经成熟了，对于ods层的数据和表同步我们要实现自动化，这样方便治理(手动创建会因为表多而混乱)。最后感谢公司和朋友们。
## 九.代码协议
无任何协议
