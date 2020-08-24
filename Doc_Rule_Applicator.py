#!/usr/bin/python
# -*- coding:utf-8 -*-
import os, sys, time, json 
import traceback
import multiprocessing
queue = multiprocessing.JoinableQueue()
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
import Doc_Rule_Applicator_Process
proc_obj    = Doc_Rule_Applicator_Process.RulePopulate()
import similarity_info as sim 

import wrapper_super_key_no_exists as wsk
w_Obj  = wsk.WrapperNESK()

class Task(object):
    def __init__(t_self, prof_id, type_info):
        t_self.prof_id  = prof_id
        t_self.type_info = type_info

def print_exception():
        formatted_lines = traceback.format_exc().splitlines()
        for line in formatted_lines:
            print '<br>',line

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
                row_id, company_id, project_id, rule_id, rule_type, stage1, stage2, stage3, stage0, stage_1, meta_data =  item.type_info
                print 'Running ', (company_id, project_id, rule_id, rule_type)
                proc_obj.process((row_id, company_id, project_id, rule_id, rule_type, stage1, stage2, stage3, stage0, stage_1, meta_data))
            except:
                print_exception()
                thisObj.update_status([item.prof_id], 'E')
        else:
            time.sleep(2)

class Render():
    def __init__(self, core_count=2):
        self.core_count = core_count
        self.dbinfo  = {'user':'root', 'password':'tas123', 'host':'172.16.20.229', 'db':'populate_info'}
        return

    def get_connection(self, dbinfo):
        return conn_obj.MySQLdb_connection(dbinfo)
        
    
    def create_directory_home_avinash(self, company_id):
        path = '/home/avinash/superkey_resultdb_ind/{0}/'.format(company_id)    
        if not os.path.exists(path):
            os.system('mkdir -p {0}'.format(path))
        return

    def __get_company_ids(self, company_ids, first=''):
        conn, cur = self.get_connection(self.dbinfo)
        print self.dbinfo
        if first == 'first':
            qry	= "select row_id, company_id, project_id, rule_id, rule_type, stage1, stage2, stage3, stage0, stage_1, meta_data from doc_rule_populate where status in ('Q', 'P', 'N') or (status in ( 'Y', 'E') and  queue_status='Q')"
        else:
            qry	= "select row_id, company_id, project_id, rule_id, rule_type, stage1, stage2, stage3, stage0, stage_1, meta_data  from doc_rule_populate where status in ('N') or (status in ( 'Y', 'E') and  queue_status='Q')"
        cur.execute(qry)
        res = cur.fetchall()
        cur.close()
        conn.close()
        print res 
        dd = {}
        prof_ids = {}
        cids = {}
        rule_cids = {}

        for rd in res:
            row_id, company_id, project_id, rule_id, rule_type, stage1, stage2, stage3, stage0, stage_1, meta_data  = rd
            dd[str(row_id)] = rd
            if rule_type == 'DOC':
                cids[company_id] = 1
            if rule_type == 'RULE':
                rule_cids[(company_id, project_id)] = 1
        print rule_cids
        # CALL SUPER PRIMARY KEY BUILDER
        for rl_tup in rule_cids:
            self.create_directory_home_avinash(rl_tup[0])
            if str(rl_tup[0]) != '1053729':
                w_Obj.non_existing_column_combinations(*rl_tup)   
            
        for cid in cids.keys():
            self.call_similarity(cid)
        prof_ids = dd.keys() 
        if prof_ids:
            self.update_status(prof_ids, 'Q')
        return prof_ids, dd

    def call_similarity(self, company_id):
        s_Obj = sim.Similarity()
        s_Obj.insert_group_info(company_id) 
        return 

    def update_status(self, prof_ids, flag):
        conn, cur = self.get_connection(self.dbinfo)
        if flag == 'Q':
            qry	= "update doc_rule_populate set status ='%s', queue_status='N' where row_id in(%s)"%(flag, ', '.join(map(lambda x:'"'+x+'"', prof_ids))) 
        else:
            qry	= "update doc_rule_populate set status ='%s' where row_id in(%s)"%(flag, ', '.join(map(lambda x:'"'+x+'"', prof_ids))) 
        cur.execute(qry)
        conn.commit()
        cur.close()
        conn.close()
        return
        
    def run(self):
        self.procs = []
        for i in range(self.core_count):
            self.procs.append( multiprocessing.Process(target=ProcessHandler,args=(self,i,)))
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
                    self.procs.append( multiprocessing.Process(target=ProcessHandler,args=(self,i,)))
                    self.procs[-1].daemon = True
                    self.procs[-1].start()
            if alive_count == 0:break
            print 'Alive Count ', alive_count, ' / ',self.core_count
            prof_ids, p_info   = self.__get_company_ids([], first)
            print [prof_ids, p_info]
            first   = ''
            if prof_ids:
                print prof_ids
                #self.update_status(map(lambda x:str(x[0]), prof_ids), 'Q')
                #print 'doc_ids ', len(prof_ids)
                for prof_id in prof_ids:
                    type_info_dct = p_info.get(prof_id)
                    print type_info_dct  
                    if not type_info_dct:continue
                    task    = Task(prof_id, type_info_dct)
                    queue.put(task)
            time.sleep(10)

if __name__ == '__main__':
    obj = Render(int(sys.argv[1]))
    obj.run()
