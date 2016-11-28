#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author:  635767825(QQ) 
import logging, time, sys, os
import threading
import MySQLdb
from Queue import Queue
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
reload(sys)  
sys.setdefaultencoding('utf8')   


def mysql_column_list(source_db_info,schemas,tables,log):
    columns = []
    conn = MySQLdb.connect(source_db_info['host'], source_db_info['user'], source_db_info['passwd'],  'information_schema', charset="utf8");
    query = "select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA ='%s' and TABLE_NAME = '%s' order by ORDINAL_POSITION" %  (schemas,tables)
    cursor = conn.cursor()
    cursor.execute(query) 
    numrows = int(cursor.rowcount)
    for i in range(numrows):
        row = cursor.fetchone()
        columns.append(row[0])
    conn.close()
    return columns
 

def mylog(log_file):
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s %(funcName)s %(asctime)s  %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filename=log_file,
                        filemode='a')
    return logging.getLogger()



class db_source(threading.Thread):
    def __init__(self,source_db_info,schemas,tables,logfile,logpos,columns,data_queue,log):
        threading.Thread.__init__(self)
        self.data_queue = data_queue
        self.source_db_info = source_db_info
        self.logfile = logfile
        self.logpos = logpos
        self.schemas = schemas
        self.tables = tables
        self.log = log
        self.columns = columns
        
    def run(self, ):
        stream = BinLogStreamReader(connection_settings=self.source_db_info,
                                server_id=3,
                                resume_stream=True,log_file=self.logfile, log_pos=self.logpos,
                                only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
                                only_tables=self.tables,only_schemas=self.schemas,
                                blocking=True)
        for binlogevent in stream:
            #prefix = "%s:%s" % (binlogevent.schema, binlogevent.table)
            for row in binlogevent.rows:
                if isinstance(binlogevent, DeleteRowsEvent):
                    vals = row["values"]
                    sql = '''delete from %s where %s = '%s' ''' % (binlogevent.table,self.columns[0],vals[self.columns[0]])
                    self.data_queue.put(sql)
                    self.log.info(sql)
                elif isinstance(binlogevent, UpdateRowsEvent):
                    vals = row["after_values"]
                    sql = ''' update %s set  ''' % binlogevent.table
                    for i in range(len(self.columns)):
                        if i == (len(self.columns)-1):
                            sql = sql + self.columns[i] + ' = ' +'\'' + str(vals[self.columns[i]]) +'\''
                        else:
                            sql = sql + self.columns[i] + ' = ' + '\'' + str(vals[self.columns[i]]) + '\'' + ' , '
                    sql = sql + " where %s =  '%s'" % (self.columns[0],vals[self.columns[0]])
                    self.data_queue.put(sql)
                    self.log.info(sql)
                elif isinstance(binlogevent, WriteRowsEvent):
                    vals = row["values"]
                    sql = '''insert into %s values (''' % binlogevent.table
                    for i in range(len(self.columns)):  
                        if i == (len(self.columns)-1):  
                            sql = sql + '\'' + str(vals[self.columns[i]]) + '\''
                        else:
                            sql = sql + '\'' + str(vals[self.columns[i]]) + '\'' + ','
                    sql = sql + ')'  
                    self.data_queue.put(sql)  
                    self.log.info(sql)
        stream.close()
        


#取queue中数据，放置目标库执行
class db_target(threading.Thread):
    def __init__(self,data_queue, target_db_info, log ):
        threading.Thread.__init__(self)
        self.data_queue = data_queue
        self.target_db_info = target_db_info
        self.log = log

    def run(self, ):
        try:
            target_db = MySQLdb.connect(host=self.target_db_info['host'], user=self.target_db_info['user'],
                                        passwd=self.target_db_info['passwd'],
                                        db=self.target_db_info['dbname'], port=self.target_db_info['port'])
            target_cursor = target_db.cursor()
            self.log.info("conn db ok.")
        except Exception, e:
            self.log.info(str(e))
        while 1:
            sqls = self.data_queue.get()
            try:
                target_cursor.execute(sqls)
                target_db.commit()
                self.log.info('exec sql : '+str(sqls))
            except Exception, e:
                self.log.warn(str(e))


def main():
    source_db_info = {"user": "root","passwd": "123456","host": "192.168.199.101","port": 3306}
    target_db_info = {'user': 'root', 'passwd': '123456', 'dbname': 'test1','host': '192.168.199.101', 'port': 3306} 
    logfile='mysql-bin.000008'
    logpos=38018
    schemas='test'
    tables='users'
    data_queue = Queue(maxsize=8000)
    # deal_process = 5
    log = mylog('mysql-replication.log')
    
    #get table structure
    columns=mysql_column_list(source_db_info,schemas,tables,log)

    binlog_work = db_source(source_db_info,schemas,tables,logfile,logpos,columns,data_queue,log)
    binlog_work.start()


    redo_work = db_target(data_queue,target_db_info,log)
    redo_work.start()

    
    binlog_work.join()
    redo_work.join()


if __name__ == "__main__":
    main()






