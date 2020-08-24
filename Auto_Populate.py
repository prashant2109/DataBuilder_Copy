#!/usr/bin/python
# -*- coding:utf-8 -*-
import os, sys, time 
import datetime
import traceback
import multiprocessing
queue = multiprocessing.JoinableQueue()
db_queue = multiprocessing.JoinableQueue()
import traceback
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
class Task(object):
    def __init__(t_self, prof_id):
        t_self.prof_id  = prof_id

def print_exception():
        formatted_lines = traceback.format_exc().splitlines()
        for line in formatted_lines:
            print '<br>',line

import color_trycp_populate_deal
pobj    = color_trycp_populate_deal.Populate_deal()

def ProcessHandler(thisObj,cpucore):
    import os
    import gc

    os.system("taskset -c -p %d %d" % (cpucore,os.getpid()))
    while 1:
        if not  queue.empty():
            item = queue.get()
            if item == 'STOP':
                break
            try:
                if item.prof_id[1] ==  'Y':
                    cmd = 'python color_trycp_populate_deal.py %s'%(item.prof_id[0])
                    print cmd
                    os.system(cmd)
                else:
                    pobj.populate(item.prof_id[0])
            except:
                print_exception()
                print '3333333333333333333333333333333333333333333333333'
                thisObj.update_status([item.prof_id[0]], 'E')
        else:
            time.sleep(2)

class Render():
    def __init__(self, core_count=2):
        self.core_count = core_count
        self.dbinfo  = {'user':'root', 'password':'tas123', 'host':'172.16.20.229', 'db':'populate_info'}
        return

    def get_connection(self, dbinfo):
        return conn_obj.MySQLdb_connection(dbinfo)

    def __get_company_ids(self, company_ids, first=''):
        conn, cur = self.get_connection(self.dbinfo)
        if company_ids:
            qry	= "select company_id from populate_status where company_id in (%s)"%(', '.join(map(lambda x:'"'+str(x)+'"', company_ids)))
        else:
            if first == 'first':
                qry	= "select  company_id from populate_status where status in ('Q', 'P', 'N') or (status in ( 'Y', 'E') and  queue_status='Q')"
            else:
                qry	= "select  company_id from populate_status where status in ('N') or (status in ( 'Y', 'E') and  queue_status='Q')"
        cur.execute(qry)
        res = cur.fetchall()
        dd  = {}
        if res:
            sql = "select company_id, from_inc from company_doc_table_info where company_id in (%s)"%(', '.join(map(lambda x:'"'+x[0]+'"', res)))
            cur.execute(sql)
            res1 = cur.fetchall()
            for r in res1:
                company_id, from_inc    = r
                if from_inc == 'Y':
                    dd[company_id]  = 'Y'
        cur.close()
        conn.close()
        print res, map(lambda x:x[0], res)
        return map(lambda x:(x[0], dd.get(x[0], '')), res)

    def update_status(self, prof_ids, flag):
        print '@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@', prof_ids
        conn, cur = self.get_connection(self.dbinfo)
        if flag == 'Q':
            qry	= "update populate_status set status ='%s', queue_status='N' where company_id in(%s)"%(flag, ', '.join(map(lambda x:'"'+x+'"', prof_ids))) 
        else:
            qry	= "update populate_status set status ='%s'   where company_id in(%s)"%(flag, ', '.join(map(lambda x:'"'+x+'"', prof_ids))) 
        print '########################################', qry
        cur.execute(qry)
        conn.commit()
        cur.close()
        conn.close()

        
    def run(self, docids):
        if docids:
            self.procs = []
            for i in range(self.core_count):
                self.procs.append( multiprocessing.Process(target=ProcessHandler,args=(self,i,)) )
                self.procs[-1].daemon = True
                self.procs[-1].start()
            prof_ids   = self.__get_company_ids(docids)
            print prof_ids
            if prof_ids:
                print '1111111111111111111111111111111111111111111111111'
                self.update_status(map(lambda x:str(x[0]), prof_ids), 'Q')
                print 'doc_ids ', len(prof_ids)
                for prof_id in prof_ids:
                    task    = Task(prof_id)
                    queue.put(task)
            if docids:
                for i in range(self.core_count):
                    queue.put('STOP')
            while 1:
                alive_count = 0
                for i in range(self.core_count):
                    if self.procs[i].is_alive():
                        alive_count += 1
                if alive_count == 0:break
                #print 'Alive Count ', alive_count, ' / ',self.core_count
            return
        self.procs = []
        for i in range(self.core_count):
            self.procs.append( multiprocessing.Process(target=ProcessHandler,args=(self,i,)) )
            self.procs[-1].daemon = True
            self.procs[-1].start()
        first   = 'first'
        while 1:
            alive_count = 0
            for i in range(self.core_count):
                if self.procs[i].is_alive():
                    alive_count += 1
                else:
                    print 'RESTART PROCESS ', i
                    self.procs.append( multiprocessing.Process(target=ProcessHandler,args=(self,i,)) )
                    self.procs[-1].daemon = True
                    self.procs[-1].start()
            if alive_count == 0 and docids:break
            print 'Alive Count ', alive_count, ' / ',self.core_count
            prof_ids   = self.__get_company_ids([], first)
            first   = ''
            if prof_ids:
                print prof_ids
                self.update_status(map(lambda x:str(x[0]), prof_ids), 'Q')
                print 'doc_ids ', len(prof_ids)
                for prof_id in prof_ids:
                    print '2222222222222222222222222222222222222222222222222', prof_id
                    print 
                    task    = Task(prof_id)
                    queue.put(task)
            time.sleep(10)

if __name__ == '__main__':
    obj = Render(int(sys.argv[1]))
    docids  = []
    if len(sys.argv) > 2:
        docids  = sys.argv[2].split('#')
    obj.run(docids)
