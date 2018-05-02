# -*- coding: utf-8 -*-
"""
Created on Sat Apr 28 15:00:06 2018

@author: dell
"""
import pymysql
import pandas as pd
import numpy as np
from six import itervalues

def ping(func):
    """
        可以保证每次事务操作之前connection连通mysql
    """
    
    def warp(self,*args,**kwargs):     
        self.conn.ping()
#        print("11111")
        return func(self,*args,*kwargs)
    return warp

class BaseDB:
    """
    不能删除数据，只能更新数据或者创建数据
    这里定义的是增改查三个主要类型的工作
    
    method：
        _select 
    """
    __tablename__ = None
    placeholder = '%s'
    maxlimit = -1
    @staticmethod
    def escape(string):
        return '`%s`' % string
    
    
    @property
    def dbcur(self):
        raise NotImplementedError
    
    def _add_field(self,tablename=None,fieldslist=[]):
        tablename = tablename or self.__tablename__
        if fieldslist and tablename:
            for field in fieldslist:
                print(fieldslist)
                if (field == "page"):
                    fieldtype = "smallint"
                else:
                    fieldtype = "varchar"
                sql_statement = "alter table %s add %s %s default Null;" %(self.escape(tablename),field,fieldtype)
                print(sql_statement)
                self._execute(sql_statement)
                print("alter %s"%fieldtype)
        
    def _execute(self,sql_statement,paralist=None):
        """
        其中dbcur是迭代器对象，本身实现__iter__方法
        """
        if not paralist:
            
            dbcur = self.dbcur
            dbcur.execute(sql_statement)
        else:
            dbcur = self.dbcur
            dbcur.executemany(sql_statement,paralist)
        
        return dbcur
    
    def _getFields(self,tablename=None):
        dbcur = self._execute("select * from %s"%tablename)
        
        fields = [f[0] for f in dbcur.description]
        return fields
    def _existTable(self,tablename=None):
        if tablename:
            dbcur = self._execute("show tables")
            
            if (tablename.lower(),) in dbcur:
#                print("exist the table %s"%tablename)
                return True
            else:
#                print("don't exist the table %s"%tablename)
                return False
        
    def _select2dict(self,tablename=None,fields="*",where=None,offset=0,limitnum=None):
        """
            tablename 
            选择结构并返回字典类型的数据
            fields 格式可以为list 和 tuple 也可以为单独字符串或者以逗号相隔的字符串
            where 可为字典 或者 字符串 条件均可以
            limitnum 为最多拉取的记录数
            offset 为从第几条记录开始
        """
        tablename = self.__tablename__ or tablename
        
        if isinstance(fields,list) or isinstance(fields,tuple) or fields is None:
            
            fields = ','.join(x for x in fields) if fields else "*"
            
        sql_query = "select %s from %s"%(fields,tablename)
        
        if where:
            
            if isinstance(where,dict):
                where = " and ".join(str(key)+"="+str(value) for key,value in where.items())
            sql_query += " where %s"%(where)
        
        if limitnum:
            sql_query += " limit %s,%s"%(offset,limitnum)
        print(sql_query)
        dbcur = self._execute(sql_query)
        fields = [f[0] for f in dbcur.description]
#        if returndbcur:
#            return dbcur
#        if returnfields:
#            return fields
        for row in dbcur:
            yield dict(zip(fields,row))#迭代器对象不能


    def _update(self):
        pass
        
    def _insertDataFrame(self,tablename=None,data=None):
        tablename =tablename or self.__tablename__
        
        if isinstance(data,pd.DataFrame) and (not data.empty):
            
            _keys =", ".join(self.escape(key) for key in list(data.columns))
            _values = ", ".join([self.placeholder,]*len(list(data.columns)))
            sql_insert = "INSERT IGNORE INTO %s (%s) VALUES (%s)" % (tablename, _keys, _values)
            
            datalist = np.array(data).tolist()
            
            self._execute(sql_insert,datalist)
            
            self.conn.commit()
            
        else:
            raise AttributeError("please insert into the DataFrame Type!!")
        pass
    
    
    def _insert2(self, tablename=None, **values):
        tablename = self.escape(tablename or self.__tablename__)
        if values:
            _keys = ", ".join((self.escape(k) for k in values))
            _values = ", ".join([self.placeholder, ] * len(values))
            sql_query = "INSERT INTO %s (%s) VALUES (%s)" % (tablename, _keys, _values)
        else:
            sql_query = "INSERT INTO %s DEFAULT VALUES" % tablename
#        logger.debug("<sql: %s>", sql_query)

        if values:
            dbcur = self._execute(sql_query, [list(itervalues(values))])
        else:
            dbcur = self._execute(sql_query)
        return dbcur.lastrowid
class StockMysqlDB(BaseDB):
    
    def __init__(self,host="192.168.40.179",port=3306,user='root', passwd='root', db='sina_data', charset='utf8'):
        self.conn = pymysql.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset=charset)
        
    
    @property
    @ping
    def dbcur(self):
        
        return self.conn.cursor()
    
    def __del__(self):
        self.conn.close()
    
    def select(self,tablename="engine_cost",fields="*",wheredict=None,limitnum=None):
        return self._select2dict(tablename=tablename,where=wheredict,fields=fields,limitnum=limitnum)
        for x in self._select2dict(tablename=tablename,where=wheredict,fields=fields,limitnum=limitnum):
            print(x)
    def insertDataFrame(self,tablename=None,dataframe=None):
        """
        Before insertdata into table,we should make sure that the
        columnname in the mysqltable we should contains in the input
        dataframe columnname
        """
        tablename = tablename or self.__tablename__
        _columnames = self._getFields(tablename=tablename)
        
        _notcontain_name_list = [name for name in dataframe.columns if name not in _columnames]
#        print(_columnames)
#        print(_notcontain_name_list)
        self._add_field(tablename=tablename,fieldslist=_notcontain_name_list)
        
        
        self._insertDataFrame(tablename=tablename,data=dataframe)
        


class StockDayTableData(StockMysqlDB):
    """
        this class make sure that the tablename must be exist
        otherwise make the stock table
    """
    def __init__(self,tablename,host="localhost",port=3306,user='root', passwd='root', db='sina_data', charset='utf8'):
        super(StockDayTableData,self).__init__(host,port,user, passwd, db, charset)
        exist = self._existTable(tablename)
        print(exist)
        self.__tablename__ = tablename
        if not exist:
            #create unexist table ,fault if the field is error then the default value must be set
            #into the mysql table
            self._execute('''create table if not exists %s( \
            %s timestamp not null comment "tradetime", \
            %s float default 0 comment "tradeprice", \
            %s float default 0 comment "upprice", \
            %s int default 0 comment "volumn", \
            %s int default 0 comment "totalprice", \
            %s varchar(9) comment "type",  \
            %s smallint comment "page",  \
            primary key ( %s ) \
            )ENGINE=Innodb default charset=utf8;'''%(
            self.escape(tablename),
            self.escape("成交时间"),
            self.escape("成交价"),
            self.escape("价格变动"),
            self.escape("成交量(手)"),
            self.escape("成交额(元)"),
            self.escape("性质"),
            self.escape("page"),
            self.escape("成交时间")
            ))
    def record(self,stockname=None,day=None,page=None,starttime=None,endtime=None):
#        tablename = tablename or self.__tablename__
        if not self._existTable("record"):
             self._execute('''create table if not exists %s( \
            %s varchar(11) not null comment "the name of stock", \
            %s date not null comment "insert day", \
            %s smallint not null comment "insert page", \
            %s time not null comment "starttime", \
            %s time not null comment "endtime" \
            )ENGINE=Innodb default charset=utf8;'''%(
            self.escape("record"),
            self.escape("stockname"),
            self.escape("day"),
            self.escape("page"),
            self.escape("starttime"),
            self.escape("endtime")
            ))
        self._insert2(tablename="record",stockname=stockname,day=day,page=page,starttime=starttime,endtime=endtime)
        
        
if __name__=="__main__":
#    stockdb_single_ins = StockMysqlDB(host="localhost",db="sina_data")
#    bb = stockdb_single_ins.select(tablename="ad",fields=("adid","remark"),limitnum=3,wheredict={"sort":12})
#
#    cc = stockdb_single_ins.select(tablename="ad",fields=("adid","remark","sort"),limitnum=None,wheredict="sort in (12,14)")
#    for x in cc:
#        print(x)
#    dd = stockdb_single_ins._insert2(tablename="person",name="zhanll",age=10)
    #dd = stockdb_single_ins._insert2(tablename="person",**{"name":"zhanll","age":"10"})
    
    
    import h5py as hp
    import os
    file = [x for x in os.walk("splitstockname_history")]
    tablenames = [fi.replace(".hdf5","") for fi in file[0][2] if len(fi)<14]
    
    def insertinto_mysql(tablename):
        sst = StockDayTableData(host="192.168.40.179",tablename = tablename)
        def readdays(tablename):
            with hp.File("splitstockname_history/%s.hdf5"%(tablename),"r+") as f:
                return [{day:list(f[day].keys())} for day in f.keys()]
        
        days_pages = readdays(tablename)
        for daydict in days_pages:
            day = list(daydict.keys())[0]
            pages = list(daydict.values())[0]
            for page in pages:
                print("%s    ---day:%s,page:%s"%(tablename,day,page))
                dataframe = pd.read_hdf("splitstockname_history/%s.hdf5"%tablename,"%s/%s"%(day,page))
                
                starttime = dataframe["成交时间"][len(dataframe["成交时间"])-1]
                endtime = dataframe["成交时间"][0]
                
                if starttime > endtime:
                    starttime,endtime = endtime,starttime
                dataframe["成交时间"]=dataframe["成交时间"].apply(lambda x:day+" "+x)
    #            dataframe["成交额(元)"]=dataframe["成交额(元)"].apply(lambda x:x.replace(",",""))
                dataframe["成交额(元)"]=dataframe["成交额(元)"].apply(lambda x:x.replace(",",""))
                dataframe["成交量(手)"]=dataframe["成交量(手)"].apply(lambda x:x.replace(",",""))
                dataframe["page"] = page
                sst.insertDataFrame(dataframe=dataframe)
                sst.record(stockname=tablename,day=day,page=page,starttime=starttime,endtime=endtime)
    
    
    import threadpool
    task_pool = threadpool.ThreadPool(15)
    #function 2
    func_var = [([t],None) for t in tablenames]
    requests = threadpool.makeRequests(insertinto_mysql, func_var)
    [task_pool.putRequest(req) for req in requests]
    task_pool.wait()
    #function 1
#    request_list = [threadpool.makeRequests(insertinto_mysql,[(t,None)]) for t in tablenames]
#        #将每个任务放到线程池中，等待线程池中线程各自读取任务，然后进行处理，使用了map函数，不了解的可以去了解一下。  
#    map(task_pool.putRequest,request_list)  
#    #等待所有任务处理完成，则返回，如果没有处理完，则一直阻塞  
#    task_pool.poll() 
#    for tablename in tablenames:
##        tablename = "sh600513"
#        
#        insertinto_mysql(tablename)
        
        
#    dataframe = pd.read_hdf("sh600513.hdf5","2018-01-29"+"/"+"2")
#    
#    transdataframe = pd.read_hdf("%s.hdf5"%tablename,"%s/%s"%(day,page))
#
#    sst.insertDataFrame(dataframe=dataframe)