# mysql-to-mysql-realtime
##mysql单表实时同步脚本
######source_db_info 来源数据库地址
######target_db_info 目标数据库地址
######logfile        同步开始的binlog日志
######logpos         同步开始的position位置
######schemas        需要同步的schemas
######tables         需要同步的表名
######log            本地日志打印


##依赖：
##pip install pymysqlreplication

##运行命令：
##python mysql-to-mysql.py

