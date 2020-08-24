import os, sys, MySQLdb, sqlite3
import mysql.connector

class Wrapper(object):
        
    def __init__(self):
        pass

    def mysql_connection(self, db_data_lst):
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur

    def create_databases_mysql(self, database_name):
        m_conn = mysql.connector.connect(
        host="172.16.20.229",
        user="root",
        passwd="tas123"
        )
        mcur = m_conn.cursor()
        mcur.execute("CREATE DATABASE %s"%(database_name))
        return 'done'
    
    def insert_populate_info(self, company_id, doc_id, table_id):
        #db_name = 'populate_info'
        #try:
        #    self.create_databases_mysql(db_name)
        #    print ['>>>>', db_name]
        #except:pass
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'populate_info']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        #crt_qry = """CREATE TABLE IF NOT EXISTS company_doc_table_info(row_id INT NOT NULL AUTO_INCREMENT, company_id TEXT, doc_id TEXT, table_id TEXT, language_info TEXT, PRIMARY KEY (row_id));"""
        #m_cur.execute(crt_qry)
            
        #crt_qry = """CREATE TABLE IF NOT EXISTS populate_status(row_id INT NOT NULL AUTO_INCREMENT, company_id TEXT,  status TEXT, source_row_id TEXT, PRIMARY KEY (row_id))""" 
        #m_cur.execute(crt_qry)

        insert_stmt = """ INSERT INTO company_doc_table_info(company_id, doc_id, table_id, status, process_time) VALUES('%s', '%s', '%s', 'N', Now())"""%(company_id, doc_id, table_id)
        print insert_stmt
        m_cur.execute(insert_stmt)
        
        read_qry = """SELECT status  FROM populate_status where company_id='%s'"""%(company_id) 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchone()
        if not t_data:
            insert_stmt = """ INSERT INTO populate_status(company_id, status, queue_status) VALUES('%s', '%s', '%s')"""%(company_id, 'N', 'N')
            m_cur.execute(insert_stmt)
        elif t_data and (t_data not in ('Y', 'E')):
            insert_stmt = """ UPDATE  populate_status SET queue_status='Q' WHERE company_id='%s' """%(company_id)
            m_cur.execute(insert_stmt)
        elif t_data and (t_data in ('Y', 'E')):
            insert_stmt = """ UPDATE  populate_status SET status='N', queue_status='N' WHERE company_id='%s' """%(company_id)
            m_cur.execute(insert_stmt)
        
        m_conn.commit()
        return 'done'
        
    def get_list_all_companies(self):
        db_path = '/mnt/eMB_db/company_info/compnay_info.db' 
        conn  = sqlite3.connect(db_path)
        cur  =  conn.cursor()
        read_qry = 'select company_name, (project_id|| "_" ||toc_company_id) from company_info;'
        cur.execute(read_qry)
        table_data = cur.fetchall()
        conn.close()
        companyName_companyId_map = {}
        for comp in table_data:
            company_name, company_id = map(str, comp)
            companyName_companyId_map[company_id] = company_name
        return companyName_companyId_map
    
    
    def get_running_stats_div(self):
        get_company_names = self.get_list_all_companies()
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'populate_info']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """SELECT * from populate_status""" 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        completed_res_lst = []
        #inprocess_dct = {'1_1':{'user_name':'', 'ptime':'', 'Total':'0/19'%(stg_cnt), 'cn':'', 'company_id':1_1, 's_1':'N', 's_2':'N', 's_3':'N', 's_4':'N', 's_5':'N', 's_6':'N', 'sn':1}}
        inprocess_dct = {}
        sn_in = 1
        sn_c  = 0
        for row in t_data:
            stg_dct = {'s_1':[], 's_2':[], 's_3':[], 's_4':[], 's_5':[], 's_6':[]}
            all_stg_info = { 's_1_m':{}, 's_2_m':{}, 's_3_m':{}, 's_4_m':{}, 's_5_m':{}, 's_6_m':{}}
            #print row
            row_id, company_id, status, queue_status, stage1, stage2, stage3, stage4, stage5, stage6, stage7, stage8, stage9, stage10, stage11, stage12, stage13, stage14, stage15, stage16, stage17, stage18, stage19, process_time, user_name = row
            get_comp_name = get_company_names[company_id] 
            if status == 'Q':
                sn_in += 1
                st_res_dct = {'user_name':user_name, 'ptime':str(process_time), 'Total':'0/19'%(stg_cnt), 'cn':get_comp_name, 'company_id':company_id, 's_1':'Q', 's_2':'N', 's_3':'N', 's_4':'N', 's_5':'N', 's_6':'N', 'sn':sn_in}
                continue 
            stg_cnt = 0
            for idx, stg in enumerate([stage1, stage2, stage3, stage4, stage5, stage6, stage7, stage8, stage9, stage10, stage11, stage12, stage13, stage14, stage15, stage16, stage17, stage18, stage19], 1):
                if idx in (1, 2, 3):
                    stg_dct['s_1'].append(stg)
                    all_stg_info['s_1_m']['s%s'%(idx)] = stg
                elif idx in (4, 5, 6):
                    stg_dct['s_2'].append(stg)
                    all_stg_info['s_2_m']['s%s'%(idx)] = stg
                elif idx in (7, 8, 9):
                    stg_dct['s_3'].append(stg)
                    all_stg_info['s_3_m']['s%s'%(idx)] = stg
                elif idx in (10, 11, 12):
                    stg_dct['s_4'].append(stg)
                    all_stg_info['s_4_m']['s%s'%(idx)] = stg
                elif idx in (13, 14, 15):
                    stg_dct['s_5'].append(stg)
                    all_stg_info['s_5_m']['s%s'%(idx)] = stg
                elif idx in (16, 17, 18, 19):
                    stg_dct['s_6'].append(stg)
                    all_stg_info['s_6_m']['s%s'%(idx)] = stg
                if stg == 'Y':
                    stg_cnt += 1        
            st_res_dct = {'user_name':user_name, 'ptime':str(process_time), 'Total':'%s/19'%(stg_cnt), 'cn':get_comp_name, 'company_id':company_id}
            st_res_dct.update(all_stg_info)
            in_stg_cnt = 0
            for st, lst in stg_dct.iteritems():
                if 'P' in lst:
                    st_res_dct[st] = 'P'
                if 'E' in lst:
                    st_res_dct[st] = 'E'
                elif all(k == 'Y' for k in lst):
                    st_res_dct[st] = 'Y'
                    in_stg_cnt += 1
                elif all(k == 'N' for k in lst):
                    st_res_dct[st] = 'N'
            if status  == 'Y':
                sn_c += 1
                st_res_dct['sn'] = sn_c
                completed_res_lst.append(st_res_dct) 
            elif status != 'Y':
                sn_in += 1
                st_res_dct['sn'] = sn_in
                st_res_dct['Total'] = '%s/6'%(str(in_stg_cnt))
                inprocess_dct[company_id] = st_res_dct
        #print 'completed:', completed_res_lst
        #print 
        #print 'inprocess: ', inprocess_dct
        return [{'message':'done', 'complete':completed_res_lst, 'inprocess':{'1_1':inprocess_dct}}]

    def get_running_stats(self):
        get_company_names = self.get_list_all_companies()
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'populate_info']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """SELECT row_id, company_id, status, queue_status, stage1, stage2, stage3, stage4, stage5, stage6, stage7, stage8, stage9, stage10, stage11, stage12, stage13, stage14, stage15, stage16, stage17, stage18, stage19, process_time, user_name, error_message, stage0 FROM populate_status ORDER BY row_id DESC""" 
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        completed_res_lst = []
        #inprocess_dct = {'1_1':{'user_name':'', 'ptime':'', 'Total':'0/19'%(stg_cnt), 'cn':'', 'company_id':1_1, 's_1':'N', 's_2':'N', 's_3':'N', 's_4':'N', 's_5':'N', 's_6':'N', 'sn':1}}
        inprocess_dct = {}
        sn_in = 1
        sn_c  = 0
        map_d   = {
            0: 's_0',
            1: 's_1',
            2: 's_1',
            3: 's_1',
        }
        for row in t_data:
            stg_dct = {'s_1':[], 's_2':[], 's_3':[], 's_4':[], 's_5':[], 's_6':[], 's_0':[]}
            all_stg_info = { 's_1_m':{}, 's_2_m':{}, 's_3_m':{}, 's_4_m':{}, 's_5_m':{}, 's_6_m':{}, 's_0_m':{}}
            #print row
            row_id, company_id, status, queue_status, stage1, stage2, stage3, stage4, stage5, stage6, stage7, stage8, stage9, stage10, stage11, stage12, stage13, stage14, stage15, stage16, stage17, stage18, stage19, process_time, user_name, error_message, stage0 = row
            get_comp_name = get_company_names.get(company_id, '')
            if status == 'Q':
                sn_in += 1
                st_res_dct = {'user_name':user_name, 'ptime':str(process_time), 'Total':'%s/19'%(0), 'cn':get_comp_name, 'company_id':company_id, 's_1':'Q', 's_2':'N', 's_3':'N', 's_4':'N', 's_5':'N', 's_6':'N', 'sn':sn_in}
                continue 
            stg_cnt = 0
            for idx, stg in enumerate([stage0, stage1, stage2, stage3, stage4, stage5, stage6, stage7, stage8, stage9, stage10, stage11, stage12, stage13, stage14, stage15, stage16, stage17, stage18, stage19], 0):
                if idx in (0,):
                    stg_dct['s_0'].append(stg)
                elif idx in (1, 2, 3):
                    stg_dct['s_1'].append(stg)
                    all_stg_info['s_1_m']['s%s'%(idx)] = stg
                elif idx in (4, 5, 6, 7, 8):
                    stg_dct['s_2'].append(stg)
                    all_stg_info['s_2_m']['s%s'%(idx)] = stg
                elif idx in (9, 10, 11, 12):
                    stg_dct['s_3'].append(stg)
                    all_stg_info['s_3_m']['s%s'%(idx)] = stg
                elif idx in (13, 14, 15):
                    stg_dct['s_4'].append(stg)
                    all_stg_info['s_4_m']['s%s'%(idx)] = stg
                elif idx in (16, 17):
                    stg_dct['s_5'].append(stg)
                    all_stg_info['s_5_m']['s%s'%(idx)] = stg
                elif idx in (18, 19):
                    stg_dct['s_6'].append(stg)
                    all_stg_info['s_6_m']['s%s'%(idx)] = stg
                if stg == 'Y':
                    stg_cnt += 1        
            st_res_dct = {'user_name':user_name, 'ptime':str(process_time), 'Total':'%s/20'%(stg_cnt), 'cn':get_comp_name, 'company_id':company_id}
            st_res_dct.update(all_stg_info)
            in_stg_cnt = 0
            for st, lst in stg_dct.iteritems():
                if 'P' in lst:
                    st_res_dct[st] = 'P'
                if 'E' in lst:
                    st_res_dct[st] = 'E'
                elif all(k == 'Y' for k in lst):
                    st_res_dct[st] = 'Y'
                    in_stg_cnt += 1
                elif all(k == 'N' for k in lst):
                    st_res_dct[st] = 'N'
            if status  == 'Y':
                sn_c += 1
                st_res_dct['sn'] = sn_c
                completed_res_lst.append(st_res_dct) 
            elif status != 'Y':
                sn_in += 1
                st_res_dct['sn'] = sn_in
                st_res_dct['Total'] = '%s/6'%(str(in_stg_cnt))
                inprocess_dct[company_id] = st_res_dct
        return [{'message':'done', 'complete':completed_res_lst, 'inprocess':{'1_1':inprocess_dct}}]

    def get_company_id_pass_company_name(self, project_id):
        db_path  = '/mnt/eMB_db/company_info/compnay_info.db'
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        read_qry = 'SELECT company_name, toc_company_id FROM company_info WHERE project_id="%s";'%(project_id)
        cur.execute(read_qry)
        table_data = cur.fetchall()
        conn.close()
        c_details = {}  
        for row in table_data:
            company_name, toc_company_id = map(str, row)
            c_details[project_id +'_'+toc_company_id] = company_name
        return c_details

    def get_sheet_id_map(self):
        db_file     = '/mnt/eMB_db/node_mapping.db'
        conn, cur   = conn_obj.sqlite_connection(db_file)
        sql   = "select sheet_id, node_name from node_mapping where review_flg = 0"
        try:
            cur.execute(sql)
            tres        = cur.fetchall()
        except:
            tres    = []
        conn.close()
        ddict = dd(set)
        for tr in tres:
            sheet_id, node_name = map(str, tr)
            ddict[sheet_id] = node_name
        return ddict


    def get_docid_docname_map(self, company_id):
         
        m_conn = MySQLdb.connect('172.16.20.229', 'root', 'tas123', 'tfms_urlid_%s'%(company_id))
        m_cur = m_conn.cursor() 
        read_qry = """ SELECT document_id, document_name FROM ir_document_master WHERE active_status='Y' """       
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        did_dn_map = {str(row[0]):str(row[1]) for row in t_data}
        return did_dn_map
        
 
    def get_remaining_tables(self, company_id, classified_set, source_table_data):
        project_id, url_id = company_id.split('_')
        did_dcn_map = self.get_docid_docname_map(company_id)
        import model_view.read_norm_cell_data as Slt_normdata
        sObj = Slt_normdata.Slt_normdata()
        data_lst = sObj.slt_normresids(project_id, url_id)
        all_table_dct = {}
        for row in data_lst:
            did, pg, tid = map(str, row)
            all_table_dct[tid] = did
        #import model_view.cp_company_docTablePh_details as py
        #pObj = py.Company_docTablePh_details()
        #all_table_dct = pObj.get_docId_passing_tableId(company_id)
        get_remaining = set(all_table_dct) - set(classified_set)
        res_lst = []
        for tab in get_remaining:   
            get_dc = all_table_dct.get(tab, '')
            get_dn = did_dcn_map.get(get_dc, '')
            source_info = source_table_data.get(tab, {})
            data_dct = {'t':tab, 'd':get_dc, 'tt':'Not Classified', 'dn':get_dn, 'act_f':'N', 'source_info':source_info}
            res_lst.append(data_dct)
        return res_lst

    def get_classified_tables_info(self, ijson):
        company_id = ijson['company_id']
        project_id, deal_id = company_id.split('_')

        sheet_id_map    = self.get_sheet_id_map()
        db_file     = '/mnt/eMB_db/%s/%s/tas_company.db'%(company_name, model_number)
        conn = sqlite3.connect(db_file)
        cur  = conn.cursor()

        try:
            cur.execute(sql)
            tres        = cur.fetchall()
        except:
            tres    = []

        conn.close()
        source_table_data = {}
        if project_id in ('20', ):
           source_table_data =  self.source_table_information(company_id)    
            
        data_rows = []
        classified_set = {}
        for row in tres:
            row = map(str, row)
            sheet_id, doc_id, doc_name, table_id_str = row
            table_id_li = table_id_str.split('^!!^')
            for tid in table_id_li:
                if not tid.strip():continue
                tup = tuple([tid, sheet_id_map.get(sheet_id, '')])
                ac = 'N'
                if tup in actual_tables:
                    ac = 'Y'
                classified_set[tid] = doc_id
                source_info = source_table_data.get(tid, {})
                data_rows.append({'t':tid, 'd':doc_id, 'tt':sheet_id_map.get(sheet_id, ''), 'dn':doc_name, 'act_f':ac, 'source_info':source_info})
        
        get_remaining = self.get_remaining_tables(company_id, classified_set, source_table_data)
        data_rows += get_remaining
        return [{'message':'done', 'data':data_rows}]
    
    def check_classification_info(self, company_id, pop_tables, only_docs, classified_tables):
        s_tab_str = ', '.join({"'"+e+"'" for e in pop_tables})
        doc_str   = ', '.join({"'"+e+"'" for e in only_docs})
            
        db_name = 'tfms_urlid_%s'%(company_id)
        db_data_lst = ['172.16.20.229', 'root', 'tas123', db_name]
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        pt_data, od_data = (), ()
        
        
        read_dn = """ SELECT document_id, document_name FROM ir_document_master """ 
        m_cur.execute(read_dn) 
        d_data = m_cur.fetchall()
        did_dn_map = {}
        for row in d_data:
            document_id, document_name = map(str, row)
            did_dn_map[document_id] = document_name            


        if pop_tables:
            read_qry = """ SELECT docid, norm_resid, pageno, source_table_info FROM norm_data_mgmt WHERE source_table_info in (%s) """%(s_tab_str)
            m_cur.execute(read_qry)
            pt_data = m_cur.fetchall() 
        if only_docs:
            read_qry = """ SELECT docid, norm_resid, pageno, source_table_info FROM norm_data_mgmt WHERE docid in (%s) """%(doc_str)
            m_cur.execute(read_qry)
            od_data = m_cur.fetchall()
        m_conn.close()
        
        res_lst  = []
        for data_tup in [pt_data, od_data]:
            for row in data_tup:
                docid, norm_resid, pageno, source_table_info = row
                docid, norm_resid, pageno = map(str, [docid, norm_resid, pageno])
                tt = 'Not Classified'
                get_tt = classified_tables.get(norm_resid, '')
                if get_tt:
                    tt = get_tt
                dt_dct = {'t':norm_resid, 'd':doc_id, 'tt':tt, 'dn':did_dn_map[docid], 'source_info':source_table_info}   
                res_lst.append(dt_dct)
        return res_lst

    def doc_table_count(self, ijson):
        import model_view.pr_company_docTablePh_details_tableType as py
        obj = py.Company_docTablePh_details()
        company_id = ijson['company_id']
        project_id, deal_id = company_id.split('_')
        company_name = self.get_list_all_companies()[company_id]
        doc_table_info = obj.get_tableList_passing_doc(company_id)
        classified_tables = self.get_table_sheet_map(company_name, project_id, company_id) 

        from_auto_inc   = ijson.get('from_inc', 'N') 
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'populate_info']
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        if from_auto_inc == 'P':
            read_qry = """ SELECT doc_id, table_id FROM company_doc_table_info WHERE stage0='P' AND from_inc!='D' AND company_id='%s' """%(company_id)    
        elif from_auto_inc == 'Y':
            read_qry = """ SELECT doc_id, table_id FROM company_doc_table_info WHERE stage0='Y' AND staus='P' AND from_inc!='D' AND company_id='%s' """%(company_id)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        
        pop_tables = {}
        only_docs = {}
        for row in t_data:
            doc_id, table_str = row
            if not table_str:
                only_docs[doc_id] = 1
            for pg_grd in  table_str.split('#'):
                pg, grd = pg_grd.split('-')
                pop_tables[(str(doc_id), str(pg), str(grd))] = 1

        res_lst = self.check_classification_info(company_id, pop_tables, only_docs, classified_tables)
        return [{'message':'done', 'data':res_lst}]





if __name__ == '__main__':
        
    obj = Wrapper() 
    print obj.get_running_stats()



