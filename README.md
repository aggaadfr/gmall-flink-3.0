# 项目说明

大数据项目之电商实时数仓3.0版本



# 使用框架

- maven-3.6.3
- jdk1.8
- flink-
- hbase
- phoeinx
- redis
- clickhouse
- mysql-5.7



# 项目结构层次说明

```
gmall-flink-3.0
└── gmall-realtime   																	# 实时模块
│   ├── pom.xml
│   ├── src
│   │   ├── main
│   │   │   ├── java
│   │   │   │   └── com
│   │   │   │       └── atguigu
│   │   │   │           ├── app
│   │   │   │           │   ├── dim
│   │   │   │           │   │   └── DimApp.java
│   │   │   │           │   ├── dwd
│   │   │   │           │   │   ├── db
│   │   │   │           │   │   └── log
│   │   │   │           │   ├── dws
│   │   │   │           │   └── func
│   │   │   │           ├── bean
│   │   │   │           ├── common
│   │   │   │           └── utils
│   │   │   └── resources
│   │   │       ├── hbase-site.xml									# hbase配置文件
│   │   │       └── log4j.properties								# flink日志打印配置文件 
│   │   └── test
│   │       └── java
│   └── target
│       ├── classes
│       │   ├── com
│       │   │   └── atguigu
│       │   │       └── app
│       │   │           └── dim
│       │   │               └── DimApp.class
│       │   ├── hbase-site.xml
│       │   └── log4j.properties	
│       └── generated-sources
│           └── annotations
└── pom.xml
```

