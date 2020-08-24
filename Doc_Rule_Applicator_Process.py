import os, sys, subprocess, json, traceback, hashlib, logging, shelve
import datetime 
from multiprocessing import Pool
############# Built In ##############

import db.get_conn as get_conn
conn_obj    = get_conn.DB()

import populate_table_lets_DB as pt
p_Obj = pt.Populate()
    
import raw_preview_builder_data_db_prod as tpyf
import raw_preview_builder_data_db_indv_prod as ipyf 
 
import raw_preview_builder_data as rpbd
v_Obj = rpbd.Validate()

import equality_info_app as eia      
e_Obj = eia.Validate()

import numeq_table_lets_DB
nobj = numeq_table_lets_DB.Table_Lets_DB() 

import raw_preview_builder_data_db_indv_tablewise_prod as indv_t
nr_Obj = indv_t.Validate() 

############# Custom ################

def disableprint():
    sys.stdout = open(os.devnull, 'w')
    pass
def enableprint():
    sys.stdout = sys.__stdout__
    pass

def print_exception():
        formatted_lines = traceback.format_exc().splitlines()
        for line in formatted_lines:
            print '<br>',line
def collect_exception():
    formatted_lines = traceback.format_exc().splitlines()
    return formatted_lines

def rule_processing(args_tup):
    exe_obj, company_id, project_id, rule_id, row_id_dct = args_tup
    s1_flg, super_key, rule_data, csv_flg = exe_obj.rule_stage1(company_id, project_id, rule_id, row_id_dct)
    if not s1_flg:return
    s2_flg, applicator_op = exe_obj.rule_stage2(company_id, project_id, rule_id, row_id_dct, super_key, rule_data, csv_flg)
    if not s2_flg:return 
    s3_flg = exe_obj.rule_stage3(company_id, project_id, rule_id, row_id_dct, applicator_op)
    if not s3_flg:return
    return 1
    
def doc_processing(args_tup):
    exe_obj, company_id, dc, meta_data, rule_id_set = args_tup
    if 0:  # NEED To DISCUSS
        s_1_flg = exe_obj.stage_1_func(company_id, dc, meta_data, rule_id_set)        
        if not s_1_flg:return 
        s0_flg = exe_obj.stage0_func(company_id, dc, rule_id_set)
        if not s0_flg:return
    s1_flg = exe_obj.stage1_func(company_id, dc, rule_id_set)
    if not s1_flg:return 
    s2_flg = exe_obj.stage2_func(company_id, dc, rule_id_set)
    if not s2_flg:return 
    s3_flg = exe_obj.stage3_func(company_id, dc, rule_id_set)
    if not s3_flg:return 
    return 1


class RulePopulate:
    def __init__(self):
        self.populate_db = {
                            "host": "172.16.20.229",
                            "user": "root",
                            "password": "tas123",
                            "db": "populate_info"
                            }     
    def process(self, ptup):
        row_id, company_id, project_id, rule_id, rule_type, stage1, stage2, stage3, stage0, stage_1, meta_data =  ptup
        row_id  = str(row_id)
        rule_id = str(rule_id)
        ptup    = (row_id, company_id, project_id, rule_id, rule_type, stage1, stage2, stage3, stage0, stage_1, meta_data)
        self.row_id = row_id
        self.t_path     = tpyf.Validate()
        self.indv     = ipyf.Validate()
            
        print [company_id, rule_id, rule_type]
        if 1:
            e_flg = 1 
            self.update_status(row_id, 'P', 'process_s_time', ['stage_1', 'stage0', 'stage1', 'stage2', 'stage3'])
            if rule_type == 'RULE':
                flg = self.populate_rule(ptup)
                if not flg:
                    e_flg = 0
            elif rule_type == 'DOC':
                flg  = self.populate_doc(ptup) 
                if not flg:
                    e_flg = 0
            if e_flg:
                self.update_status(row_id, 'Y', 'process_time')
            else:
                self.update_status(row_id, 'E', 'process_time')

    def get_connection(self, dbinfo):
        return conn_obj.MySQLdb_connection(dbinfo)
 
    def sqlite_connection(self, dbinfo):
        return conn_obj.sqlite_connection(dbinfo)

    def update_status(self, row_id, flag, tupdate=None, stages=[]):
        conn, cur = self.get_connection(self.populate_db)
        ststr   = ', '.join(map(lambda x:x+"='N'", stages))
        if ststr:
            ststr   = ', '+ststr
            
        if tupdate:
            qry	= "update doc_rule_populate set status ='%s', %s=Now() %s where row_id='%s' "%(flag, tupdate, ststr, row_id) 
        else:
            qry	= "update doc_rule_populate set status ='%s' %s where row_id='%s' "%(flag, ststr, row_id) 
        
        cur.execute(qry)
        conn.commit()
        cur.close()
        conn.close()

    def update_status_stage(self, row_id, flag, stage, stage_flg):
        conn, cur = self.get_connection(self.populate_db)
        qry	= "update doc_rule_populate set status ='%s', %s='%s' where row_id='%s' "%(flag, stage, stage_flg, row_id) 
        print qry
        cur.execute(qry)
        conn.commit()
        cur.close()
        conn.close()


    def update_only_stage_doc(self, stage, stage_flg, row_ids):
        row_str = ', '.join(row_ids)
        conn, cur = self.get_connection(self.populate_db)
        qry	= "update doc_rule_populate set %s='%s' where row_id in (%s) "%(stage, stage_flg, row_str) 
        cur.execute(qry)
        conn.commit()
        cur.close()
        conn.close()

    def update_only_stage_rule(self, stage, stage_flg, row_ids):
        row_str = ', '.join(row_ids)
        conn, cur = self.get_connection(self.populate_db)
        qry	= "update doc_rule_populate set %s='%s' where row_id in (%s) "%(stage, stage_flg, row_str) 
        print qry
        cur.execute(qry)
        conn.commit()
        cur.close()
        conn.close()

    def get_quid(self, text):
        m = hashlib.md5()
        m.update(text)
        quid = m.hexdigest()
        return quid

    def rule_stage1(self, company_id, project_id, rule_id, row_id_dct):
        # read_table_tagg({'company_id':1053729, 'project_id':5, 'table_types':['81'], 'table_ids':[], 'row_ids':[]})
        super_key, rule_data, csv_flg = '', [], ''
        try:
            self.update_only_stage_rule('stage1', 'P', row_id_dct) 
            if 0:#str(company_id) == '1053729':
                ijson = {'company_id':company_id, 'project_id':project_id, 'table_types':[str(rule_id)], 'table_ids':[], 'row_ids':[]}
                print ijson
                res_tup = p_Obj.read_table_tagg(ijson)
                super_key, rule_data, csv_flg = res_tup
                print (super_key, csv_flg)
            #else:
            if 1:
                sys.path.insert(0, '/root/tablets/tablets_mapping/pysrc/')
                import super_key_rule_populate as sk
                sk_Obj = sk.SuperKeyInfo()
                ijson = {'company_id':company_id, 'project_id':project_id, 'table_types':[str(rule_id)], 'table_ids':[], 'row_ids':[]}
                super_key, rule_data, csv_flg = sk_Obj.prep_super_key_csv_info(ijson)
                print (super_key, csv_flg, rule_data)
                sys.path.insert(0, '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/')
        except: 
            self.update_only_stage_rule('stage1', 'E', row_id_dct) 
            print_exception()

            exp_data = collect_exception()
            logging.basicConfig(filename='error_logs/%s_error.log'%(company_id), filemode='a', format='%(message)s', level=logging.INFO)        
            logging.info('************* RULE GENERATION **************')
            dt = str(datetime.datetime.now())
            logging.info(dt)
            logging.info('PROJECT-ID-{0}, RULE-ID-{1}'.format(project_id, rule_id))
            logging.info(exp_data)
            logging.info('*********************************\n')
            return 0, super_key, rule_data, csv_flg
        else:
            self.update_only_stage_rule('stage1', 'Y', row_id_dct) 
            return 1, super_key[0], rule_data, csv_flg

    def rule_stage2(self, company_id, project_id, rule_id, row_id_dct, super_key, rule_data, csv_flg):
        applicator_op = []
        try:
            self.update_only_stage_rule('stage2', 'P', row_id_dct) 
            applicator_op = nr_Obj.apply_table_wise(company_id, rule_data, super_key, csv_flg)
        except: 
            self.update_only_stage_rule('stage2', 'E', row_id_dct) 
            print_exception()
            exp_data = collect_exception()
            logging.basicConfig(filename='error_logs/%s_error.log'%(company_id), filemode='a', format='%(message)s', level=logging.INFO)        
            logging.info('************* APPLICATOR **************')
            dt = str(datetime.datetime.now())
            logging.info(dt)
            logging.info('PROJECT-ID-{0}, RULE-ID-{1}'.format(project_id, rule_id))
            logging.info(exp_data)
            logging.info('*********************************\n')
            return 0, applicator_op
        else:
            self.update_only_stage_rule('stage2', 'Y', row_id_dct) 
            return 1, applicator_op
        
    def rule_stage3(self, company_id, project_id, rule_id, row_id_dct, applicator_op):
        try:
            self.update_only_stage_rule('stage3', 'P', row_id_dct) 
            sys.path.insert(0, '/root/tablets/tablets_mapping/pysrc/')
            from modules.databuilder import form_builder_from_traverse_path  as fb
            fb_Obj = fb.DataBuilder()
            ijson = {'company_id':company_id, 'project_id':project_id, 'table_type':str(rule_id), 'table_ids':[], 'row_ids':[]}
            print ijson
            db_shelve_path     = '/mnt/eMB_db/company_management/{0}/db_output/{1}.sh'.format(company_id, ijson['table_type'])
            os.system("mkdir -p "+'/'.join(db_shelve_path.split('/')[:-1]))
            print db_shelve_path
            ijson['data'] = applicator_op
            slv = shelve.open(db_shelve_path, 'n')
            slv['data'] = ijson['data']
            slv.close()
            fb_Obj.form_builder_from_data(ijson)
            sys.path.insert(0, '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/')
        except: 
            self.update_only_stage_rule('stage3', 'E', row_id_dct) 
            print_exception()
            exp_data = collect_exception()
            logging.basicConfig(filename='error_logs/%s_error.log'%(company_id), filemode='a', format='%(message)s', level=logging.INFO)        
            logging.info('************* DATA BUILDER **************')
            dt = str(datetime.datetime.now())
            logging.info(dt)
            logging.info('PROJECT-ID-{0}, RULE-ID-{1}'.format(project_id, rule_id))
            logging.info(exp_data)
            logging.info('*********************************\n')
            return 0
        else:
            self.update_only_stage_rule('stage3', 'Y', row_id_dct) 
            return 1
        
    def populate_rule(self, ptup):
        row_id, company_id, project_id, rule_id, rule_type, stage1, stage2, stage3, stage0, stage_1, meta_data =  ptup
        argtup  = (self, company_id, project_id, rule_id, {row_id:{row_id:1}})
        flg     = rule_processing(argtup)
        return flg

    def stage_1_func(self, company_id, doc_str, meta_data, rule_id_set):
        try:
            self.update_only_stage_doc('stage_1', 'P', rule_id_set) 
            os.chdir('/root/tas_cloud/WorkSpaceBuilder_DB/pysrc/')
            try:
                meta_data = json.loads(meta_data)
                page_dct = map(str, meta_data['pages'])
                page_str = '#'.join(page_dct)
            except:page_str = ''

            if not page_str:
                cmd_1 = """ python runpostinc.py {0} {1} """.format(company_id, doc_str)
            elif page_str:
                cmd_1 = """ python run_post_inc_page1.sh {0} {1} {2} """.format(company_id, doc_str, page_str)
            print cmd_1
            process = subprocess.Popen(cmd_1, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            os.chdir('/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/')
            err_data = error[1]
            if err_data:
                print err_data
                EEEEEEEEEEEEEEEEEEEEEEEEEEEEEE           
        except: 
            print_exception()
            self.update_only_stage_doc('stage_1', 'E', rule_id_set) 
            exp_data = collect_exception()
            logging.basicConfig(filename='error_logs/%s_error.log'%(company_id), filemode='a', format='%(message)s', level=logging.INFO)        
            logging.info('************* POST INC STAGE-1 **************')
            dt = str(datetime.datetime.now())
            logging.info(dt)
            logging.info(doc_str)
            logging.info(page_str)
            logging.info(err_data)
            logging.info(exp_data)
            logging.info('*********************************\n')
            #sys.exit('ERROR')
            return 0
        else:
            self.update_only_stage_doc('stage_1', 'Y', rule_id_set) 
            return  1

    def stage0_func(self, company_id, doc_str, rule_id_set):
        try:
            self.update_only_stage_doc('stage0', 'P', rule_id_set) 
            os.chdir('/root/tas_cloud/WorkSpaceBuilder_DB/pysrc/')
            cmd_1 = """ python similargroup_cmd.py {0} {1} """.format(company_id, doc_str)
            print cmd_1
            process = subprocess.Popen(cmd_1, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            os.chdir('/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/')
            err_data = error[1]
            if err_data:
                print err_data
                EEEEEEEEEEEEEEEEEEEEEEEEEEEEEE           
        except: 
            print_exception()
            self.update_only_stage_doc('stage0', 'E', rule_id_set) 
            exp_data = collect_exception()
            logging.basicConfig(filename='error_logs/%s_error.log'%(company_id), filemode='a', format='%(message)s', level=logging.INFO)        
            logging.info('************* POST INC SIMILARITY **************')
            dt = str(datetime.datetime.now())
            logging.info(dt)
            logging.info(doc_str)
            logging.info(err_data)
            logging.info(exp_data)
            logging.info('*********************************\n')
            #sys.exit('ERROR')
            return 0
        else:
            self.update_only_stage_doc('stage0', 'Y', rule_id_set) 
            return  1
        
        
    def stage1_func(self, company_id, doc_str, rule_id_set):
        try:
            cmd = """ python update_altered_tables.py {0} {1} """.format(company_id, doc_str)
            process1 = subprocess.Popen(cmd, stderr=subprocess.PIPE, shell=True)

            self.update_only_stage_doc('stage1', 'P', rule_id_set) 
            os.chdir('/root/tablets/tablets_mapping/pysrc/')
            #sys.path.insert(0, '/root/tablets/tablets_mapping/pysrc')
            cmd_1 = """ python populate_table_info.py {0} {1} """.format(company_id, doc_str)
            print cmd_1
            process = subprocess.Popen(cmd_1, stderr=subprocess.PIPE, shell=True)
            error = process.communicate()
            os.chdir('/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/')
            err_data = error[1]
            if  err_data:
                print err_data
                EEEEEEEEEEEEEEEEEEEEEEEEEEEEEE           
            '''
            import populate_table_info as pti
            p_Obj = pti.INC_Company_Mgmt()
            ijson = {"company_id":self.company_id, "doc_ids":doc_str.split('#')}
            p_Obj.populate_table_information(ijson)
            sys.path.insert(0, '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/')
            '''

        except: 
            print_exception()
            self.update_only_stage_doc('stage1', 'E', rule_id_set) 
            #self.update_status('E')
            exp_data = collect_exception()
            logging.basicConfig(filename='error_logs/%s_error.log'%(company_id), filemode='a', format='%(message)s', level=logging.INFO)        
            logging.info('************* POPULATE TABLE INFO **************')
            dt = str(datetime.datetime.now())
            logging.info(dt)
            logging.info(doc_str)
            logging.info(err_data)
            logging.info(exp_data)
            logging.info('*********************************\n')
            #sys.exit('ERROR')
            return 0
        else:
            self.update_only_stage_doc('stage1', 'Y', rule_id_set) 
            return  1

    def stage2_func(self, company_id, doc_str, rule_id_set):
        try:
            self.update_only_stage_doc('stage2', 'P', rule_id_set) 
            for did in doc_str.split('#'):
                v_Obj.raw_builder_forfedata_tablelet(company_id, did, 'tablets')
                dir1  = "/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/"+company_id+"/"+str(did)+"/numeq/"
                dir2  = "/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/"+company_id+"/"+str(did)+"/label/"
                shdir = "/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/"+company_id+"/"+str(did)+"/ctree/"
                nobj.form_num_label_info( {"company_id":company_id, "doc_id":int(did)}, dir1, dir2, shdir)
        except: 
            print_exception()
            self.update_only_stage_doc('stage2', 'E', rule_id_set) 
            exp_data = collect_exception()
            logging.basicConfig(filename='error_logs/%s_error.log'%(company_id), filemode='a', format='%(message)s', level=logging.INFO)        
            logging.info('************* _DATA **************')
            dt = str(datetime.datetime.now())
            logging.info(dt)
            logging.info(doc_str)
            logging.info(exp_data)
            logging.info('*********************************\n')
            #sys.exit('ERROR')
            return 0
        else:
            self.update_only_stage_doc('stage2', 'Y', rule_id_set) 
            return 1

    def stage3_func(self, company_id, doc_str, rule_id_set):
        try:
            self.update_only_stage_doc('stage3', 'P', rule_id_set) 
            e_Obj.applicator_tablet(company_id, doc_str)
            e_Obj.display_applicator_tablet(company_id, doc_str)
        except: 
            print_exception()
            self.update_only_stage_doc('stage3', 'E', rule_id_set) 
            #self.update_status('E', 'stage3')
            exp_data = collect_exception()
            logging.basicConfig(filename='error_logs/%s_error.log'%(company_id), filemode='a', format='%(message)s', level=logging.INFO)        
            logging.info('************* EQUALITY INFO APP **************')
            dt = str(datetime.datetime.now())
            logging.info(dt)
            logging.info(doc_str)
            logging.info(exp_data)
            logging.info('*********************************\n')
            #sys.exit('ERROR')
            return 0
        else:
            self.update_only_stage_doc('stage3', 'Y', rule_id_set) 
            cmd = """ python delete_altered_tables.py {0} {1} """.format(company_id, doc_str)
            process = subprocess.Popen(cmd, stderr=subprocess.PIPE, shell=True)
            return 1
        
    def populate_doc(self, ptup):
        row_id, company_id, project_id, rule_id, rule_type, stage1, stage2, stage3, stage0, stage_1, meta_data =  ptup
        argtup  = (self, company_id, rule_id, meta_data, {row_id:1})
        return doc_processing(argtup)

    def run_stage(self, company_id, project_id, rule_id, rtype):
        conn, cur = self.get_connection(self.populate_db)
        sql = "select row_id from doc_rule_populate where  company_id=%s and project_id=%s and rule_id=%s and  rule_type='%s'"%( company_id, project_id, rule_id, rtype)
        print sql
        cur.execute(sql)
        res = cur.fetchone()
        row_id  = res[0]
        stage1, stage2, stage3  = 'N', 'N', 'N'
        rp_Obj.process((row_id, company_id, project_id, rule_id, rtype, stage1, stage2, stage3))
        
        
    

if __name__ == '__main__':  
    rp_Obj = RulePopulate()
    rp_Obj.run_stage(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
