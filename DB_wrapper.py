import os, sys, sqlite3

class DBWrapper:

    def read_db_ref_data(self, conn, cur):
        read_qry = """ SELECT rawdb_row_id, xml_id, bbox, page_coords, doc_id, page_no, table_id FROM reference_table; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
    
        ref_data = {}
        #all_docs = {}
        for row_data in t_data:
            rawdb_row_id, xml_id, bbox, page_coords, doc_id, page_no, table_id = row_data
            xml_sp = filter(lambda x: x, xml_id.split('#'))
            np = page_no
            if xml_sp:
                xml_sp1 = filter(lambda x: x, xml_sp[0].split('z'))
                if xml_sp1:
                    np = xml_sp1[-1].split('_')[1]
            #print 'rrrr', np
            ref_data[rawdb_row_id] = {'x':xml_id, 'bx':bbox, 'pc':page_coords, 'd':doc_id, 'p': np, 't':table_id}
            #all_docs[doc_id] = 1
        return ref_data

    def read_db_phcsv(self, conn, cur):
        read_qry = """ SELECT rawdb_row_id, period_type, period, currency, scale, value_type FROM phcsv_info; """ 
        cur.execute(read_qry)
        t_data = cur.fetchall()
        phcsv_dct = {}
        for row_data in t_data:
            rawdb_row_id, period_type, period, currency, scale, value_type = row_data
            phcsv_dct[rawdb_row_id] = {'pt':period_type, 'p':period, 'c':currency, 's':scale, 'v':value_type}
        return phcsv_dct    

    def read_data_builder_info(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        table_type = ijson['table_type']
        db_path = '/mnt/eMB_db/company_management/{0}/data_builder/{1}/data_builder.db'.format(company_id, project_id)
        conn = sqlite3.connect(db_path) 
        cur  = conn.cursor()
        read_qry = """ SELECT row_id, taxo_group_id, src_row, src_col, value, cell_type, cell_ph FROM data_builder WHERE table_type='%s'; """%(table_type)
        cur.execute(read_qry)
        t_data = cur.fetchall()
        ref_data = self.read_db_ref_data(conn, cur)
        phcsv_dct = self.read_db_phcsv(conn, cur)         
        conn.close()        
        
        db_row_col_dct = {}
        collect_col_wise_grp = {}
        for row_data in t_data:
            row_id, taxo_group_id, src_row, src_col, value, cell_type, cell_ph = row_data
            cell_ref_dct = ref_data[row_id]
            cell_xml  = cell_ref_dct['x']
            table_id  = cell_ref_dct['t']
            db_row_col_dct.setdefault(int(src_row), {})[int(src_col)] = {'v':value, 'x':cell_xml, 't':table_id, 'ct':cell_type, 'cell_ph':cell_ph, 'tx_id':taxo_group_id, 'row_id':row_id}
            collect_col_wise_grp.setdefault(int(src_col), {})[taxo_group_id] = 1
            
        db_src_rows = db_row_col_dct.keys()     
        collect_col_wise_ph = {}
        hgh_cols = {}
        data_lst = []
        sn = 1
        for s_row in db_src_rows:
            row_dct = {'rid':sn, 'cid':sn, 'sn':{'v':sn}}
            col_info = db_row_col_dct[s_row]
            for col, cell_dct in col_info.iteritems():
                val = cell_dct['v']
                xm = cell_dct['x']
                t_b_id = cell_dct['t']
                tab_row_id = cell_dct['row_id']
                tx_id   = cell_dct['tx_id']
                c_type  = cell_dct['ct']
                c_ph    = cell_dct['cell_ph']
                p_dct = phcsv_dct.get(tab_row_id, {})
                collect_col_wise_ph.setdefault(col, set()).add(c_ph)
                if c_type == 'HGH': 
                    hgh_cols[col] = 1
                val_dct_i = {'v':val, 'title':p_dct, 'x':xm}
                row_dct[col] = val_dct_i
            data_lst.append(row_dct)
            sn+=1
        col_def_lst = [{'v_opt': 3, 'k': "checkbox", 'n': ""}]
        all_cols = sorted(collect_col_wise_grp.keys())
        for c_l in all_cols:
            g_i_str = ' '.join(collect_col_wise_grp[c_l])
            p_h_lst = collect_col_wise_ph[c_l] 
            p_h = ''
            if col not in hgh_cols:
                p_h = '~'.join(p_h_lst)
            k_dct = {'k':c_l, 'n':p_h, 'g':g_i_str}
            col_def_lst.append(k_dct)
        if 0:
            for dtdct in data_lst:
                for ph_dct in col_def_lst:
                    k_i = ph_dct['k']
                    if k_i in dtdct:
                        print '\t\t\t CELL', k_i, '-----', dtdct[k_i]['v']    
                print 'RRRRRRRRRRRRR'
  
        res = {'data': data_lst, 'phs':col_def_lst}
        return res 
        
    def read_all_table_types(self, ijson):
        company_id = ijson['company_id']
        project_id = ijson['project_id']
        db_path = '/mnt/eMB_db/company_management/{0}/{1}/table_info.db'.format(company_id, project_id)
        conn = sqlite3.connect(db_path) 
        cur  = conn.cursor()
        read_qry = """ SELECT DISTINCT(classified_id) FROM scoped_gv; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        table_types = {tt[0] for tt in t_data} 
        return table_types
        

if __name__ == '__main__':
    db_Obj = DBWrapper()      
    ijson = {"company_id":1053730, "project_id":"5", "table_type":"170"}
    #print db_Obj.read_data_builder_info(ijson)
    print db_Obj.read_all_table_types(ijson)









