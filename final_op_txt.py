import os, sys, json, copy, sqlite3
import datetime
import utils.convert as scale_convert
sconvert_obj   = scale_convert.Convert()
import utils.numbercleanup as numbercleanup
numbercleanup_obj   = numbercleanup.numbercleanup()
import compute_period_and_date
c_ph_date_obj   = compute_period_and_date.PH()
import db.get_conn as get_conn
conn_obj    = get_conn.DB()
import pyapi as pyf
p_Obj = pyf.PYAPI()
class Generate_Project_Txt(object):
    
    def __init__(self):
        self.month_map = {'January':1, 'February':2, 'March':3, 'April':4, 'May':5, 'June':6, 'July':7, 'August':8, 'September':9, 'October':10, 'November':11, 'December':12}
        
    def mysql_connection(self, db_data_lst):
        import MySQLdb
        host_address, user, pass_word, db_name = db_data_lst 
        mconn = MySQLdb.connect(host_address, user, pass_word, db_name)
        mcur = mconn.cursor()
        return mconn, mcur

    def connect_to_sqlite(self, db_path):
        import sqlite3
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        return conn, cur

    def read_all_ass_no(self, new_cid, company_id, company_name):
        #self.inc_db_name = 'AECN_INC'
        db_docid    = {}
        if 1:
            print (company_name, company_id.split('_')[0])
            m_tables, rev_m_tables, doc_m_d,table_type_m = p_Obj.get_main_table_info(company_name, company_id.split('_')[0], [])
            tmpdoc_d    = {}
            print 'doc_m_d ', doc_m_d
            for k, v in doc_m_d.items():
                print 'DB ',k, v
                db_docid[str(v)]    = 1
        if company_id.split('_')[0] not in ['20', '1']:
            db_data_lst = ['172.16.20.52', 'root', 'tas123', 'DataBuilder_%s'%(new_cid)] 
            m_conn, m_cur = self.mysql_connection(db_data_lst)
        else:
            db_data_lst = ['172.16.20.52', 'root', 'tas123', self.inc_db_name] 
            m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """  SELECT doc_id, meta_data FROM batch_mgmt_upload where status = '10N' ; """
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        doc_ass_dct = {}
        for row_data in t_data:
            doc_id, meta_data = row_data
            if str(doc_id) not in db_docid:continue
            try:
                meta_data = json.loads(meta_data)
            except:continue
            tmpdd   = {}
            for k, v in meta_data.items():
                tmpdd[' '.join(k.replace('u00a0', '').strip('.').split())]  = v
            meta_data   = tmpdd

            acc_no      = meta_data.get('SEC Accession No')
            if not acc_no:
                acc_no      = meta_data.get('SECAccessionNo')
            tasacc_no   = meta_data.get('TAS_SEC_ACC_No')
            if tasacc_no:
                acc_no  = tasacc_no
            print ['EEEEEEEE', doc_id, acc_no, tasacc_no]
            if not acc_no:continue
            acc_no = acc_no.replace('u00a0', '')
            acc_no  = acc_no.strip().strip('.')
            #print ['EEEEEEEE', doc_id, acc_no, tasacc_no]
            doc_ass_dct[acc_no] = doc_id
            doc_ass_dct[('D', str(doc_id))] = acc_no
        #print doc_ass_dct
        return doc_ass_dct    
            
    def read_company_name_from_company_mgmt(self, new_cid, pid, client_name, client_display_name):
        cdn = ''.join([es for es in client_display_name.split()])
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db_test'] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT cd.client_name, cm.sec_name, cd.client_id, cm.sec_cik, cm.financial_year_end  FROM company_mgmt AS cm INNER JOIN client_details AS cd ON cm.row_id=cd.company_id WHERE cm.row_id=%s AND cd.project_id=%s;  """%(new_cid, pid)
        print read_qry
        m_cur.execute(read_qry)
        t_data = m_cur.fetchone()   
        m_conn.close()
        #print t_data
        txt_path = '/var/www/html/KBRA_output/{0}/{1}'.format(pid, new_cid)
        if not os.path.exists(txt_path):
            os.system('mkdir -p "%s"'%(txt_path))        
        file_path = os.path.join(txt_path, 'CompanyDetails.txt')
        #f = open('/var/www/html/KBRA_output/MAndTBank/KeyBankingCapitalandProfitabilityFigures/CompanyDetails.txt', 'w')
        f = open(file_path, 'w')
        header = '\t'.join(['KBRACompanyName', 'KBRACompanyID', 'FilingCompanyName'])
        f.write(header+'\n')
        #print [t_data]
        company_display_name, sec_name, client_id, sec_cik, fye  = map(str, t_data)
        data_info = '\t'.join((company_display_name, client_id, sec_name))     
        f.write(data_info+'\n')
        f.close()
        return sec_cik, client_id, fye, file_path 

    def insert_document_master(self, m_conn, m_cur, company_name, db_id, cid):
        db_cid, model_number    = db_id.split('_')
        db_path = '/mnt/eMB_db/{0}/{1}/tas_company.db'.format(company_name, model_number)
        conn, cur  = self.connect_to_sqlite(db_path)
        read_qry = """ SELECT doc_id, document_type, filing_type, period, reporting_year, doc_name, doc_release_date, doc_from, doc_to, doc_download_date, doc_prev_release_date, doc_next_release_date FROM company_meta_info; """ 
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        
        insert_rows = []
        for row in t_data:
            doc_id, document_type, filing_type, period, reporting_year, doc_name, doc_release_date, doc_from, doc_to, doc_download_date, doc_prev_release_date, doc_next_release_date = row
            if doc_release_date and '-' in doc_release_date:
                drd = doc_release_date
                drd = '-'.join(drd.split('-')[::-1])
            else:   
                drd = '1970-01-01'

            if doc_from and '-' in doc_from:
                df = doc_from
                df = '-'.join(df.split('-')[::-1])
            else:   
                df = '1970-01-01'

            if doc_to and '-' in doc_to:
                dt = doc_to
                dt = '-'.join(dt.split('-')[::-1])
            else:   
                dt = '1970-01-01'

            if doc_download_date and '-' in doc_download_date:
                ddd = doc_download_date
                ddd = '-'.join(ddd.split('-')[::-1])
            else:
                ddd = '1970-01-01' 

            if doc_prev_release_date and '-' in doc_prev_release_date:
                dprd = doc_prev_release_date
                dprd = '-'.join(dprd.split('-')[::-1])
            else:
                dprd = '1970-01-01' 

            if doc_next_release_date and '-' in doc_next_release_date:
                dnrd = doc_next_release_date
                dnrd = '-'.join(dnrd.split('-')[::-1])
            else:
                dnrd = '1970-01-01'
            
            dt_tup = (cid, document_type, filing_type, period, reporting_year, doc_name, drd, df, dt, ddd, dprd, dnrd, 'TAS-System')
            insert_rows.append(dt_tup)
        
        e_cnt = 0
        for k in insert_rows:
            print k, '\n'
            insert_stmt = """ INSERT INTO document_master(company_id, document_type, filing_type, period, year, document_name, document_release_date, document_from, document_to, document_download_date, previous_release_date, next_release_date, user_name) VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') """%(k)
            try:
                m_cur.execute(insert_stmt)
            except:
                print 'SSSSSSSS'    
                e_cnt += 1
                #continue
            m_conn.commit()
        #if insert_rows:
        #    insert_stmt = """ INSERT INTO document_master(company_id, document_type, filing_type, period, year, document_name, document_release_date, document_from, document_to, document_download_date, previous_release_date, next_release_date, user_name) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
        #    m_cur.executemany(insert_stmt, insert_rows)
        #    m_conn.commit()
        return 

    def get_doc_map(self, new_cid, pid, client_name, client_display_name, sec_cik, client_id, company_id, company_name):
        cdn = ''.join([es for es in client_display_name.split()])
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db_test'] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT assension_number, doc_id, document_release_date, filing_type, url_info FROM document_master WHERE company_id=%s; """%(new_cid)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()   
        m_conn.close()
        if not t_data:
            print 'SEC Crawling info is not available'
            xxxxxxxxxxxxxx
        
        doc_as_dct = self.read_all_ass_no(new_cid, company_id, company_name)
        #print doc_as_dct
        #sys.exit()
        
        txt_path = '/var/www/html/KBRA_output/{0}/{1}'.format(pid, new_cid)
        if not os.path.exists(txt_path):
            os.system('mkdir -p "%s"'%(txt_path))        
        finfo   = 0
        rev_map_d   = {}
        fdata   = []
        for row_data in t_data:
            assension_number, doc_id, document_release_date, filing_type, url_info = map(str, row_data) 
            #print
            print'ACCCCCCCCCCCCCCCCCCCCCCCCCC',  [doc_id, assension_number]
            tas_did = doc_as_dct.get(assension_number)
            print row_data
            print '\ttas_did ', [tas_did]
            if not tas_did:continue
            rev_map_d[str(tas_did)]   = doc_id
            tas_did = doc_id
            finfo   = 1
            fdata.append([sec_cik, client_id, 'EDGAR', assension_number, '', '%s-%s'%(new_cid, tas_did), document_release_date, filing_type])
        #sys.exit()
        return fdata,  rev_map_d, doc_as_dct
        
    def create_txt_from_document_master(self, new_cid, pid, client_name, client_display_name, sec_cik, client_id, fdata, company_id, company_name):
        txt_path = '/var/www/html/KBRA_output/{0}/{1}'.format(pid, new_cid)
        if not os.path.exists(txt_path):
            os.system('mkdir -p "%s"'%(txt_path))        
        file_path = os.path.join(txt_path, 'FilingDetails.txt')
        #f = open('/var/www/html/KBRA_output/MAndTBank/KeyBankingCapitalandProfitabilityFigures/FilingDetails.txt', 'w')
        f = open(file_path, 'w')
        header = '\t'.join(['CIK', 'KBRACompanyID', 'FilingSource', 'Assension Number', 'OtherFilingID', 'TAS FilingID', 'FilingDate', 'SECFormType'])
        f.write(header+'\n')
        for data_info in fdata:
            f.write('\t'.join(data_info)+'\n')
        f.close()
        if  not fdata:
            print 'Empty Filing'
            xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
        return file_path

        cdn = ''.join([es for es in client_display_name.split()])
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db_test'] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT assension_number, doc_id, document_release_date, filing_type, url_info FROM document_master WHERE company_id=%s; """%(new_cid)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()   
        m_conn.close()
        
        doc_as_dct = self.read_all_ass_no(new_cid, company_id, company_name)
        #print doc_as_dct
        #sys.exit()
        
        txt_path = '/var/www/html/KBRA_output/{0}/{1}'.format(pid, new_cid)
        if not os.path.exists(txt_path):
            os.system('mkdir -p "%s"'%(txt_path))        
        file_path = os.path.join(txt_path, 'FilingDetails.txt')
        #f = open('/var/www/html/KBRA_output/MAndTBank/KeyBankingCapitalandProfitabilityFigures/FilingDetails.txt', 'w')
        f = open(file_path, 'w')
        header = '\t'.join(['CIK', 'KBRACompanyID', 'FilingSource', 'Assension Number', 'OtherFilingID', 'TAS FilingID', 'FilingDate', 'SECFormType'])
        f.write(header+'\n')
        finfo   = 0
        rev_map_d   = {}
        for row_data in t_data:
            assension_number, doc_id, document_release_date, filing_type, url_info = map(str, row_data) 
            tas_did = doc_as_dct[assension_number]
            print
            print row_data
            print '\ttas_did ', [tas_did]
            if not tas_did:continue
            rev_map_d[str(tas_did)]   = doc_id
            tas_did = doc_id
            finfo   = 1
            data_info = '\t'.join([sec_cik, client_id, 'EDGAR', assension_number, '', '%s-%s'%(new_cid, tas_did), document_release_date, filing_type])   
            f.write(data_info+'\n')
        f.close()
        if  finfo == 0:
            print 'Empty Filing'
            xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
        #sys.exit()
        return file_path,  rev_map_d
        
    def cp_read_norm_data_mgmt(self, company_id):
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'tfms_urlid_%s'%(company_id)] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT norm_resid, source_table_info FROM norm_data_mgmt; """
        m_cur.execute(read_qry)
        t_data = m_cur.fetchall()
        m_conn.close()
        res_data    = {}
        for row in t_data:
            if row[1]:
                res_data[row[0]]    = eval(row[1])
        return res_data
        
    def read_from_info(self, m_cur, m_conn, dpg, new_cid):
        print [dpg]
        read_qry = """ SELECT userid FROM db_data_mgmt_grid_slt WHERE docid=%s AND pageno=%s AND groupid=%s; """%dpg
        print read_qry
        m_cur.execute(read_qry)
        info_ref = m_cur.fetchone()
        #print [info_ref, read_qry]
        if not info_ref and str(new_cid) in ('1372', '1400', '1378', '1401'):
            info_ref = ('From Sentence', '')
        info_ref = info_ref[0]
        return info_ref
        
    def read_period_ending(self, year_ending, ph):
        print [year_ending]
        y_end_num   = int(datetime.datetime.strptime(year_ending, '%B').strftime('%m'))
        return c_ph_date_obj.get_date_from_ph(ph, y_end_num)
        
    def read_company_meta_info(self, company_name, model_number):
        txt_path = '/mnt/eMB_db/%s/%s/company_meta_info.txt'%(company_name, model_number)
        f = open(txt_path)
        data = f.readlines()
        f.close()
        res_dct = {}
        header = data[0]
        r_data = data[1]
        header = header.split('\t')
        r_data = r_data.split('\t')
        val = ''
        ck_dct = {'Jan':'January', 'Feb':'February', 'Mar':'March', 'Apr':'April', 'May':'May', 'Jun':'June', 'Jul':'July', 'Aug':'August', 'Sep':'September', 'Oct':'October', 'Nov':'November', 'Dec':'December'}
        for ix, r_key in enumerate(header):
            if r_key == 'FY':   
                val = r_data[ix]
                
        val = val.split('-')
        val = val[1].strip()
        fye = ck_dct[val]
        return fye

    def read_template_info_dic(self):
        db_path = '/mnt/eMB_db/template_info.db'
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        read_qry = 'SELECT row_id, sheet_name, project_name, display_name, industry, time_series from meta_info'
        cur.execute(read_qry)
        table_data = cur.fetchall()
        conn.close()
        template_dct = {}
        for row in table_data:
            row_id, sheet_name, project_name, display_name, industry, time_series = row[:]
            template_dct.setdefault((project_name, industry), {})[str(row_id)]  = row
        return template_dct
        
    def get_mneumonic_info(self, ijson):
        template_dct = self.read_template_info_dic()
        project_name  = ijson.get('project_name', '')
        industry_type = ijson.get('industry_type', '')
        templates       = template_dct.get((project_name, industry_type), {})
        m_line_items = {}
        for tmpid in templates.keys():
            self.read_mapped_ttypes_mneumonic(copy.deepcopy(ijson), tmpid, templates[tmpid], m_line_items, p_Obj)
        return m_line_items

    def split_by_camel(self, txt):
        if isinstance(txt, unicode) :
            txt = txt.encode('utf-8')  
        if ' ' in txt:
            return txt
        txt_ar  = []
        for c in txt:
            if c.upper() == c:
                txt_ar.append(' ')
            txt_ar.append(c)
        txt = ' '.join(''.join(txt_ar).split())
        return txt

    def read_mapped_ttypes_mneumonic(self, ijson, industry_id, row, taxo_cl, m_obj):
        tmpid, sheet_name, project_name, display_name, industry, time_series = row
        project_name = ''.join(project_name.split())
        industry     = ''.join(industry.split())
        tsheet_name  = ''.join(sheet_name.split())
        k = '-'.join([industry, project_name]) 
        if k in ['FoodProcessing-EquityBuyout', 'PassengerTransportation-Airline'] and sheet_name in ['PassengerTransportation-Airline', 'Equity_Buyout_KraftHeinzCompany']:
            k = '-'.join([industry, project_name]) 
        else:
            k = '-'.join([industry, project_name, tsheet_name]) 
            
        if time_series == 'N':
            k = '-'.join([k, 'without_time_series'])
        ijson['table_type'] = k
        ph_formula_d    = m_obj.read_ph_user_formula(ijson, '')
        db_file         = "/mnt/eMB_db/template_info.db"
        conn        = sqlite3.connect(db_file)#self.connect_to_sqlite(db_file)
        cur         = conn.cursor()
        sql         = "select taxo_id, prev_id, parent_id, taxonomy, taxo_label, scale, value_type, client_taxo, yoy, editable, formula_str, sign, target_currency from industry_kpi_template where industry_id=%s"%(industry_id)
        cur.execute(sql)
        res         = cur.fetchall()
        conn.close()
        ptype_taxo  = {}
        for rr in res:
            taxo_id, prev_id, parent_id, taxonomy, taxo_label, scale, value_type, client_taxo, yoy, editable, formula_str, sign, target_currency   = rr
            if client_taxo:
                taxo_label  = self.split_by_camel(client_taxo)
            t_row_f     = ph_formula_d.get(('USER F', taxonomy), ())
            t_row_f_all = ph_formula_d.get(('ALL_USER F', taxonomy), {})
            fflag       = 'USER F'
            if not t_row_f:
                fflag       = 'F'
                t_row_f     = ph_formula_d.get(('F', taxonomy), ())
                t_row_f_all = ph_formula_d.get(('ALL_F', taxonomy), {})
            for flg in  ['CELL F', 'PTYPE F']:
                for cell_id, t_row_f in ph_formula_d.get((flg, taxonomy), {}).items():
                    ptype_taxo[taxonomy]  = 1
            if t_row_f_all:
                #print '\n============================='
                #print taxonomy
                if len(t_row_f_all.keys()):
                    done_rem  = {}
                    for f_taxos, t_row_ft in t_row_f_all.items():
                        tmpref  = []
                        for ft in t_row_ft[1]:
                            if ft['type'] != 'v':
                                if ft['op'] != '=':
                                    #if ft['t_type'] != ijson['table_type']:
                                    tmptup  = (ft['t_type'], ft['g_id'], ft['txid'])
                                    taxo_cl[tmptup] = taxo_label 
        
    def read_table_type(self):
        db_path = '/mnt/eMB_db/company_info/compnay_info.db'
        conn = sqlite3.connect(db_path)
        cur = conn.cursor() 
        read_qry = """ SELECT gen_id, table_type FROM model_id_map; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        res_dct = {str(e[0]):e[1] for e in t_data}
        return res_dct

    def read_kpi_data(self, ijson, tinfo={}):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        disp_name       = ''
        empty_line_display  = 'Y'
        if not ijson.get("template_id"):
            db_file     = '/mnt/eMB_db/page_extraction_global.db'
            conn, cur   = self.connect_to_sqlite(db_file)
            sql = "select industry_type, industry_id from industry_type_storage"
            cur.execute(sql)
            res = cur.fetchall()
            conn.close()
            exist_indus = {}
            for rr in res:
                industry_type, industry_id  = rr
                industry_type   = industry_type.lower()
                exist_indus[industry_type]  = industry_id
            tindustry_type  = ijson['table_type'].lower()
            industry_id = exist_indus[tindustry_type]
            db_file     = "/mnt/eMB_db/industry_kpi_taxonomy.db"
            conn, cur   = self.connect_to_sqlite(db_file)
        else:
            industry_id = ijson['template_id']
            db_file     = "/mnt/eMB_db/template_info.db"
            conn, cur   = self.connect_to_sqlite(db_file)
            sql = "select display_name, empty_line_display from meta_info where row_id=%s"%(industry_id)
            cur.execute(sql)
            tmpres  = cur.fetchone()
            disp_name   = tmpres[0]
            empty_line_display   = tmpres[1]
        empty_line_display  = 'Y'
        #print 'disp_name ', disp_name
        try:    
            sql = "alter table industry_kpi_template add column yoy TEXT"
            cur.execute(sql)
        except:pass
        try:    
            sql = "alter table industry_kpi_template add column editable TEXT"
            cur.execute(sql)
        except:pass
        try:
                sql = "alter table industry_kpi_template add column formula_str TEXT"
                cur.execute(sql)
        except:pass
        try:
                sql = "alter table industry_kpi_template add column taxo_type TEXT"
                cur.execute(sql)
        except:pass
        sql         = "select taxo_id, prev_id, parent_id, taxonomy, taxo_label, scale, value_type, client_taxo, yoy, editable, target_currency, mnemonic_id from industry_kpi_template where industry_id=%s and taxo_type !='INTER'"%(industry_id)
        cur.execute(sql)
        res         = cur.fetchall()
        data_d      = {}
        grp_d       = {}
        all_table_types = {}
            
        for rr in res:
            taxo_id, prev_id, parent_id, taxonomy, taxo_label, scale, value_type, client_taxo,yoy, editable, target_currency, mnemonic_id   = rr
            if client_taxo:
                taxo_label  = self.split_by_camel(client_taxo)
            #if scale in ['1.0', '1']:
            #    scale   = ''
            #if str(deal_id) == '51':
            #    scale   = ''
            grp_d.setdefault(parent_id, {})[prev_id]    = taxo_id
            data_d[taxo_id]  = (taxo_id, taxonomy, taxo_label, scale, value_type, client_taxo, yoy, editable, target_currency, mnemonic_id)
        final_ar    = []
        taxo_exists = {}
        found_open  = {}
        pc_d        = {}
        get_open_d  = {'done':"N"}
        def form_tree_data(dd, level, pid, p_ar):
            prev_id = -1
            iDs = []
            pid = -1
            done_d  = {}
            if (pid not in dd) and dd:
                ks  = dd.keys()
                ks.sort()
                pid = ks[0]

            while 1:
                if pid not in dd:break
                ID  = dd[pid]
                if (ID, pid) in done_d:break #AVOID LOOPING
                done_d[(ID, pid)]  = 1
                pid = ID
                iDs.append(ID)
            tmp_ar  = []
            prev_id = -1
            for iD in iDs:
                if p_ar:
                    pc_d.setdefault(data_d[p_ar[-1]][1], {})[data_d[iD][1]]        = 1
                final_ar.append(data_d[iD]+(level, pid, prev_id))
                if tinfo and (tinfo.get(data_d[iD][1], {}).get('desc', {}).get('f') == 'Y' or tinfo.get(data_d[iD][1], {}).get('desc', {}).get('fm') == 'Y'):
                    for tmpid in p_ar+[iD]:
                        taxo_exists[data_d[tmpid][1]] = 1
                    if get_open_d['done'] == 'N':
                        for tmpid in p_ar+[iD]:
                            found_open[data_d[tmpid][1]]    = 1
                            
                        
                c_ids   = grp_d.get(iD, {})
                if c_ids:
                    form_tree_data(c_ids, level+1, iD, p_ar+[iD])
                prev_id = iD
                if level == 1 and found_open:
                    get_open_d['done']  = "Y"
        root    = grp_d[-1]
        form_tree_data(root, 1, -1, [])
        return final_ar, taxo_exists, found_open, pc_d, disp_name, empty_line_display



    def create_db_gv_txt(self, company_id, ijson, sec_cik, client_id, fye, project_name, project_display_name, new_cid, pid, rev_doc_map_d, doc_as_dct):
        print 'rev_doc_map_d', rev_doc_map_d
        
        cdn = ''.join([es for es in project_display_name.split()])
        company_name    = ijson['company_name']
        model_number    = ijson['model_number'] 
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        ijson['template_id'] = 10
        company_id      = "%s_%s"%(project_id, deal_id)


        db_file = "/mnt/eMB_db/company_info/compnay_info.db"
        conn, cur   = conn_obj.sqlite_connection(db_file)
        sql = "select table_type, description from model_id_map"
        cur.execute(sql)
        res = cur.fetchall()
        conn.close()
        ttype_map_d = {}
        for r in res:
            tt, description = r
            ttype_map_d[tt] = description
        db_file         = '/mnt/eMB_db/%s/%s/mt_data_builder.db'%(company_name, model_number)
        conn, cur   = conn_obj.sqlite_connection(db_file)
        read_qry = 'SELECT table_type, group_id, display_name FROM tt_group_display_name_info;'
        cur.execute(read_qry)
        table_data = cur.fetchall()
        for row in table_data:
            ttable_type, tgroup_id, display_name = row[:]
            if display_name:
                ttype_map_d[ttable_type]    = display_name
        
        final_ar, taxo_exists, found_open, pc_d, disp_name, empty_line_display  = self.read_kpi_data(ijson)

        scale_dct = {'mn':'Million', 'th':'Thousands', 'bn':'Billion'}
        vt_dct    = {'MNUM':'Monetary Number', 'BNUM':'Cardinal Number'}
        txt_dir_path = '/var/www/html/DB_Model/{0}/{1}/Norm_Scale/'.format(company_name, cdn)
        list_txt_files = os.listdir(txt_dir_path) 

        #fye = self.read_company_meta_info(company_name, model_number)
        res_data = self.cp_read_norm_data_mgmt(company_id)
        if company_id.split('_')[0] not in ['20', '1']:
            db_data_lst = ['172.16.20.52', 'root', 'tas123', 'DataBuilder_%s'%(new_cid)] 
            m_conn, m_cur = self.mysql_connection(db_data_lst)
        else:
            db_data_lst = ['172.16.20.52', 'root', 'tas123', self.inc_db_name] 
            m_conn, m_cur = self.mysql_connection(db_data_lst)
        header_lst = ['KBRACompanyID', 'Filing ID', 'PeriodEnding', 'PeriodDuration', 'PeriodType', 'Data_Point_Name','Mnemonic', 'Source Type', 'Source Section', 'Value: Full Form', 'Value: As Reported', 'Value: Unit As Reported', 'Value: Type', 'Value: Currency', 'Value: Duration', 'Value: Calculated']
        header_str = '\t'.join(header_lst)
        fp_path = '/var/www/html/KBRA_output/{0}/{1}/Data.txt'.format(pid, new_cid)
        f1 = open(fp_path, 'w')
        f1.write(header_str+'\n')
        taxo_template_map = self.get_mneumonic_info(ijson)

        gen_id_map_tt = self.read_table_type()
        done_docs   = {}
        for txt_file in list_txt_files:
            txt_file = txt_file.strip()
            if '-P' not in txt_file:continue
            if '414-P.txt' != txt_file:continue
            gr_id =  txt_file.split('-')    
            gen_id = gr_id[0]
            if len(gr_id) == 3:
                gr_id = gr_id[1:-1]
                gr_id = ''.join(gr_id)
            else:gr_id = ''
            txt_path = os.path.join(txt_dir_path, txt_file)
            f = open(txt_path)
            txt_data = f.read()
            f.close()
            txt_data = eval(txt_data)
            import pyapi as pyf
            p_Obj = pyf.PYAPI()
            for table_type, tt_data in  txt_data.iteritems():
                #ijson['table_type'] = table_type
                #ijson['grpid'] = gr_id
                data     = tt_data['data']
                key_map  = tt_data['key_map']
                rc_keys = data.keys()  
                rc_keys.sort()
                mneumonic_txt_d = {}
                map_d   = {}
                ph_cols = {}
                for rc_tup in rc_keys:
                    dt_dct = data[rc_tup]
                    
                    if rc_tup[1]  == 0:
                        mneumonic_txt_d[dt_dct[21]] = dt_dct[1]
                        map_d[dt_dct[21]]   = rc_tup[0]
                        #map_d[('REV', rc_tup[0])]   = dt_dct[21]
                    else:
                        ph_cols[rc_tup[1]]  = dt_dct[10]
                        #map_d.setdefault(map_d[('REV', rc_tup[0])], {}).append(rc_tup)
            
                
                phs = ph_cols.keys()
                phs.sort()
                for row in final_ar:
                    mneumonic_txt   =  row[2]
                    mneumonic_id   =  row[9]
                    rowid   = map_d.get(row[1], -1)
                    for colid in phs:
                        rc_tup  = (rowid, colid)
                        g_dt_dct = data.get(rc_tup, {})
                        formula     = g_dt_dct.get(15, [])
                        op_ttype    = {}
                        taxo_d      = []
                        for f_r in formula[:1]: 
                            for r in f_r:
                                if r['op'] == '=' or r['type'] != 't':continue
                                op_ttype[r['ty']] = 1
                                taxo_d.append(r['ty'])
                        if len(taxo_d) > 1:
                            re_stated_all   = []
                        else:
                            re_stated_all = g_dt_dct.get(31, [])
                        year_wise_d = {}
                        idx_d   = {}
                        for r in re_stated_all:
                            #print '\t', r
                            if(r.get(2)):
                                if r[2] not in idx_d:
                                    idx_d[r[2]]   = len(idx_d.keys())+1
                                year_wise_d.setdefault(r.get(2), []).append(r)
                        
                        if not year_wise_d:
                            if re_stated_all:
                                continue
                                print 'Error ', (rc_tup, re_stated_all)
                                sys.exit()
                            year_wise_d[1]  = [g_dt_dct]
                            idx_d[1]    = 1
                        values  = year_wise_d.keys()
                        values.sort(key=lambda x:idx_d[x])
                        for v1 in values:
                            dt_dct  = year_wise_d[v1][0]
                            formula     = g_dt_dct.get(15, [])
                            op_ttype    = {}
                            taxo_d      = []
                            docids      = {}
                            scale_d     = {}
                            ttype_d     = {}
                            for f_r in formula[:1]: 
                                #print
                                for r in f_r:
                                    #print r
                                    if r['op'] == '=' or r['type'] != 't':continue
                                    if r['label'] == 'Not Exists':continue
                                    if r['doc_id']:
                                        if str(r['doc_id']) not in rev_doc_map_d:
                                            print 'DOC NOT Matching ', [mneumonic_txt, g_dt_dct.get(10),r['doc_id'], doc_as_dct.get(('D', str(r['doc_id'])))]
                                            xxxxxxxxxxxxxxxxxxxxxxxxx
                                        docids[rev_doc_map_d[str(r['doc_id'])]]      = 1
                                    if r.get('v'):
                                        scale_d[str(r['phcsv']['s'])]     = 1
                                    ttype_d[ttype_map_d[r['tt']]]    = 1
                                    op_ttype[ttype_map_d[r['tt']]] = 1
                                    taxo_d.append(r['ty'])
                            if len(taxo_d) > 1:
                                gv_txt      = dt_dct.get(2, '')
                            else:
                                gv_txt      = dt_dct.get(38, '')
                            tmpgv_txt  = numbercleanup_obj.get_value_cleanup(gv_txt)
                            if (gv_txt == '-' and  not tmpgv_txt) or (gv_txt == '--' and  not tmpgv_txt) or (gv_txt == 'n/a'  and  not tmpgv_txt):
                                tmpgv_txt = '-'
                            if gv_txt and not tmpgv_txt:
                                print 'Error Clean Value', [gv_txt, tmpgv_txt]  
                                sys.exit()
                            gv_txt  = tmpgv_txt 
                            
                                
                            #print
                            #print (row, colid)
                            #{1: '8,792.03', 2: '8792.03', 3: '13681', 4: '170', 5: [[383, 215, 43, 7]], 6: '2013FY', 7: u'Mn', 8: 'MNUM', 9: 'USD', 10: 'FY2013', 39: {'p': '2013', 's': 'TH', 'vt': 'MNUM', 'c': 'USD', 'pt': 'FY'}, 34: '', 14: {'d': '13681', 'bbox': [[46, 215, 27, 7]], 'v': 'Amount', 'x': 'x28_170@0_6', 'txt': u'Tier1capital - Amount', 't': '219'}, 40: '', 24: '219', 25: 'x29_170@0_11', 26: 'PeriodicFinancialStatement-FY2013', 38: '$ 8,792,035'}
                            #if len()
                            clean_value     = dt_dct.get(2, '')
                            cln_val         = copy.deepcopy(clean_value)
                            currency        = dt_dct.get(9, '')
                            if len(taxo_d) > 1:
                                scale           = dt_dct.get(7, '')
                            else:
                                scale           = dt_dct.get(39, {}).get('s', '')
                                if not scale:
                                    scale           = dt_dct.get(7, '')
                            scale1          = dt_dct.get(7, '')
                            tmp_ttype       = table_type
                            calc_value = dt_dct.get(41, '')
                            if op_ttype:#len(op_ttype.keys()) == 1:
                                tmp_ttype   = op_ttype.keys()[0]

                            value_type = dt_dct.get(8, '')
                            restated_lst = dt_dct.get(40, []) 
                            rep_rst_flg  =  'Original'
                            if restated_lst == 'Y':
                                rep_rst_flg = 'Restated' 
                            if len(values) > 1 and idx_d[v1] > 1:
                                rep_rst_flg = 'Restated' 
                            ph_info = ph_cols[colid]
                            pdate, period_type, period = '', '', ''
                            if ph_info:
                                print [fye, ph_info,  dt_dct.get(3, '')]
                                pdate = self.read_period_ending(fye, ph_info)
                                #print pdate
                                period_type = ph_info[:-4]
                                period  = ph_info[-4:]
                            #print [ph_info, pdate]
                            doc_id = dt_dct.get(3, '')
                            doc_data = dt_dct.get(27, [])
                            if doc_id:
                                if str(doc_id) not in rev_doc_map_d:
                                    print 'DOC NOT Matching ', [doc_id, doc_as_dct.get(('D', str(doc_id)))]
                                    xxxxxxxxxxxxxxxxxxxxxxxxx
                                doc_id  = rev_doc_map_d[str(doc_id)]
                            #if doc_data:pass
                                #doc_data = doc_data[0][0]
                                #doc_id  = doc_data['doc_id']
                            if len(taxo_d) > 1:# or rc_tup not in data:
                                calc_value  = 'Y'
                                if len(ttype_d.keys()) > 1:
                                    tmp_ttype   = ''
                                if len(scale_d.keys()) > 1:
                                    scale       = ''
                                    scale1       = ''
                                gv_txt      = ''
                                if len(docids.keys()) > 1:
                                    doc_id      = ''
                            if rc_tup not in data:
                                tmp_ttype   = ''
                                scale       = ''
                                scale1       = ''
                                gv_txt      = ''
                                doc_id      = ''
                            if str(scale1) not in ('1', ''):
                                tv, factor  = sconvert_obj.convert_frm_to_1(scale.lower(), '1', clean_value if not gv_txt else numbercleanup_obj.get_value_cleanup(gv_txt))
                                #sys.exit()
                                if factor:
                                    clean_value = float(tv.replace(',', ''))
                                    clean_value = str(clean_value)
                                    clean_value = p_Obj.convert_floating_point(clean_value)
                                    clean_value = clean_value.replace(',', '')
                            if not clean_value:
                                rep_rst_flg = ''
                            if len(taxo_d) > 1  and len(scale_d.keys())  > 1:# or rc_tup not in data:
                                    scale       = ''
                                    scale1       = ''
                            table_id  = dt_dct.get(24, '')
                            info_ref = '' 
                            if table_id and doc_id:
                                dpg = res_data.get(int(table_id), '')
                                print [dpg, table_id]
                                info_ref = self.read_from_info(m_cur, m_conn, dpg, new_cid)
                            #mneumonic_txt = mneumonic_txt.decode('utf-8')
                            #print [mneumonic_txt], mneumonic_txt
                            try:
                                mneumonic_txt = mneumonic_txt.encode('utf-8')
                            except:
                                mneumonic_txt = str(mneumonic_txt)
                            #if value_type != 'Percentage':value_type = 'Absolute'
                            if info_ref:
                                if info_ref != 'From Sentence':
                                    info_ref = 'Table'
                                if info_ref == 'From Sentence':
                                    info_ref = 'Text'
                            #if len(taxo_d) > 1:# or rc_tup not in data:
                            #    scale   = ''
                            vaur = scale_dct.get(scale.lower(), scale)
                            vt_c = vt_dct.get(value_type, value_type)
                            print ['SSSSSSSSSS', vt_c, value_type, mneumonic_txt], '\n'
                            tmpcalc_value   = 'false'
                            if calc_value == 'Y':
                                tmpcalc_value   = 'true'
                                vaur    = ''
                                gv_txt  = ''
                            dcname  = ''
                            if doc_id:
                                dcname  = '%s-%s'%(new_cid, doc_id)
                            if 0:#len(docids.keys()) > 1:
                                print 'Error More than One docs in formula ', [mneumonic_txt, mneumonic_id, pdate, str(period_type), docids]
                                xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
                            if dcname == '' and gv_txt:# and len(docids.keys()) == 1:
                                print 'Error Document Mapping not found ', [doc_id]
                                xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
                            done_docs[dcname]   = 1
    
                            dt_lst = [client_id, dcname, pdate, str(period_type), rep_rst_flg, mneumonic_txt, mneumonic_id, info_ref, str(tmp_ttype), str(clean_value), str(gv_txt), str(vaur), vt_c, currency, str(period_type), tmpcalc_value]
                            if clean_value and (not vaur.strip()) and  tmpcalc_value == 'false':
                                print 'Error empty scale for reported value ', dt_lst , scale_d, taxo_d
                                xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
                            print 'MMMMMMMMMMM', dt_lst , scale_d
                            print [taxo_d, dt_dct.get(2, ''), dt_dct.get(38, '')],'\n'
                            info_str = '\t'.join(dt_lst)
                            f1.write(info_str+'\n')
        f1.close()        
        return fp_path, done_docs

    def read_db_name(self, project_id):
        db_data_lst = ['172.16.20.52', 'root', 'tas123', 'WorkSpaceDb_DB'] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT ProjectCode FROM ProjectMaster WHERE ProjectID='%s'; """%(project_id)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchone()   
        m_conn.close()
        db_name = t_data[0]
        return db_name
        
    def get_company_details(self, new_cid, project_id):
        db_data_lst = ['172.16.20.229', 'root', 'tas123', 'project_company_mgmt_db_test'] 
        m_conn, m_cur = self.mysql_connection(db_data_lst)
        read_qry = """ SELECT DB_id,company_display_name, company_name FROM company_mgmt WHERE row_id=%s; """%(new_cid)
        m_cur.execute(read_qry)
        t_data = m_cur.fetchone()
        DB_id = ''
        company_display_name    = ''
        company_name            = ''
        if t_data:
            DB_id = t_data[0]
            company_display_name    = t_data[1]
            company_name            = t_data[2]
        r_qry = """ SELECT client_name, display_name FROM client_mgmt WHERE row_id=%s; """%(project_id)
        m_cur.execute(r_qry)
        t_data = m_cur.fetchone()
        m_conn.close()
        client_name, client_display_name = t_data
        db_path = '/mnt/eMB_db/company_info/compnay_info.db' 
        conn  = sqlite3.connect(db_path)
        cur  =  conn.cursor()
        project_id, deal_id = DB_id.split('_')
        read_qry = """ select company_name from company_info WHERE project_id='%s' AND toc_company_id='%s'; """%(project_id, deal_id)
        cur.execute(read_qry)
        table_data = cur.fetchone()
        conn.close()
        company_name = table_data[0]
        return DB_id, client_name, client_display_name, company_display_name, company_name
        
    def create_ijson_with_db_data(self, DB_id, ijson):
        print [DB_id]
        project_id, deal_id = DB_id.split('_')
        db_path = '/mnt/eMB_db/company_info/compnay_info.db' 
        conn  = sqlite3.connect(db_path)
        cur  =  conn.cursor()
        read_qry = """ select company_name from company_info WHERE project_id='%s' AND toc_company_id='%s'; """%(project_id, deal_id)
        cur.execute(read_qry)
        table_data = cur.fetchone()
        conn.close()
        company_name = table_data[0]
        ijson['company_name'] = company_name
        ijson['model_number'] = project_id
        ijson['project_id'] = project_id
        ijson['deal_id']  = deal_id
        return 
        
    def create_zip_file(self, cmp_path, doc_path, final_data_path, new_cid, pid, cmp_name, client_id, datetime):
        print cmp_path
        print doc_path
        print final_data_path
        from zipfile import ZipFile
        file_path = '/var/www/html/KBRA_output/{0}/{1}/{2}_{3}.zip'.format(pid, new_cid, client_id, datetime)
        cmd = 'zip -j "%s" "%s" "%s" "%s"'%(file_path, cmp_path, doc_path, final_data_path)
        print cmd
        os.system(cmd)
        return file_path
        print file_path
        with ZipFile(file_path, 'w') as zp:
            zp.write(cmp_path)
            zp.write(doc_path)
            zp.write(final_data_path)
        return file_path
        
        
    def generate_all_txt_files(self, new_cid, project_id, datetime_val):
        ijson = {}
        self.inc_db_name = 'AECN_INC' #self.read_db_name(new_cid)
        print '-----*20'
        print [new_cid]
        company_id, project_name, project_display_name, cmp_name, cmp_name1 = self.get_company_details(new_cid, project_id)
        print [company_id]
        ### Company Details
        sec_cik, client_id, fye, cmp_path = self.read_company_name_from_company_mgmt(new_cid, project_id, project_name, project_display_name)
        #print [sec_cik, client_id, fye, cmp_path]
        #doc_path, rev_doc_map_d = self.create_txt_from_document_master(new_cid, project_id, project_name, project_display_name, sec_cik, client_id)
        doc_data, rev_doc_map_d, doc_as_dct = self.get_doc_map(new_cid, project_id, project_name, project_display_name, sec_cik, client_id, company_id, cmp_name1)
        #print 'MMMMMMMMMMMMMMMMM', rev_doc_map_d
        self.create_ijson_with_db_data(company_id, ijson)
        #fye = self.month_map.get(fye, 12)
        final_data_path, done_docs =  self.create_db_gv_txt(company_id, ijson, sec_cik, client_id, fye, project_name, project_display_name, new_cid, project_id, rev_doc_map_d, doc_as_dct)
        print final_data_path 
        doc_data    = filter(lambda x:x[5] in done_docs, doc_data)
        doc_path = self.create_txt_from_document_master(new_cid, project_id, project_name, project_display_name, sec_cik, client_id, doc_data, company_id, cmp_name1)
        f_path = self.create_zip_file(cmp_path, doc_path, final_data_path, new_cid, project_id, cmp_name, client_id, datetime_val)
        print f_path
        res = [{'message':'done', 'path':f_path}]
        return res 
        
if __name__ == '__main__':
    r_Obj = Generate_Project_Txt()
    new_cid, project_id, datetime_val = sys.argv[1], sys.argv[2], sys.argv[3]
    r_Obj.generate_all_txt_files(new_cid, project_id, datetime_val)
    ##r_Obj.read_all_ass_no()
    
    #ijson = {"cmd_id":58,"company_name":"MAndTBank","deal_id":"149","from_merge":"N","grp_doc_ids":{},"industry_type":"Financial Services","model_number":"20","pid":"1","project_id":"20","project_name":"Key Banking Capital and Profitability Figures","table_type":"LoanCharge-offAndAllowanceForCreditLosses","taxo_flg":0,"user":"prashant","year":15}
    #print r_Obj.get_company_details(1012, 1)
    #print r_Obj.read_for_raw_db(ijson)
    #print r_Obj.read_period_ending('December', 'FY2016')
    #print r_Obj.read_company_meta_info()
    #print r_Obj.create_db_gv_txt('20_149', ijson)
    #print r_Obj.create_txt_for_data_builder_data(ijson)
    ##print r_Obj.read_company_name_from_company_mgmt(1369)
    ##print r_Obj.create_txt_from_document_master(1369)
    ######################################
