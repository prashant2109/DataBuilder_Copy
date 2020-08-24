import os, sys, sqlite3, json, copy

class Formula_storage:

    def read_rawdb(self, conn, cur):
        read_qry = """ SELECT row_col_groupid, gvxmlid, gridids FROM rawdb; """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        rcg_tab_xml_dct = {}
        for row_data in t_data:
            row_col_group, gvxmlids, gridids = row_data
            table_id = '_'.join(gridids.split('#')[:3])    
            xml = ' '.join(eval(gvxmlids))
            rcg_tab_xml_dct[row_col_group] =  '{0}^{1}'.format(table_id, xml)
        return rcg_tab_xml_dct  
        
    def prep_insert_structure(self, conn, cur, rcg_tab_xml_dct):
        read_qry = """ SELECT resultant_info, opernands, groupid, formula_type, formula_sign, formula_key, fid, formula FROM formulainfo;  """
        cur.execute(read_qry)
        t_data = cur.fetchall()
        insert_rows = []
        for row_data in t_data:
            resultant_info, operand_lst = map(eval, row_data[:2])
            groupid = row_data[2]
            formula_type, formula_sign, formula_key, fid = row_data[3:7]
            formula = row_data[7]
            res_rc = resultant_info[1]
            res_rcg = '{0}_{1}_{2}'.format(res_rc[0], res_rc[1], groupid)
            table_xml = rcg_tab_xml_dct.get(res_rcg, '')
            op_lst = []
            for op_tup in operand_lst:
                op_rc = op_tup[0]
                op_rcg = '{0}_{1}_{2}'.format(op_rc[0], op_rc[1], groupid)
                op_table_xml = rcg_tab_xml_dct.get(op_rcg, '')
                op_lst.append(op_table_xml)
            
            formula_i = ''
            if formula_type == 'FORMULA NONG':
                formula_i = copy.deepcopy(formula) 
            insert_tup = (fid, table_xml, json.dumps(op_lst), groupid, formula_type, formula_sign, formula_key, formula_i)
            insert_rows.append(insert_tup)
        if insert_rows:
            try:
                drop_stmt = """ DROP TABLE formula_table_xml;  """
                cur.execute(drop_stmt)
            except:pass
            crt_stmt = """ CREATE TABLE IF NOT EXISTS formula_table_xml(row_id INTEGER PRIMARY KEY AUTOINCREMENT, fid TEXT, resultant_rcg TEXT, operand_rcg TEXT, groupid TEXT, formula_type TEXT, formula_sign TEXT, formula_key TEXT, formula TEXT);  """
            cur.execute(crt_stmt)
            insert_stmt = """  INSERT INTO formula_table_xml(fid, resultant_rcg, operand_rcg, groupid, formula_type, formula_sign, formula_key, formula) VALUES(?, ?, ?, ?, ?, ?, ?, ?);  """
            cur.executemany(insert_stmt, insert_rows)
            conn.commit()
        return

    def read_formulainfo(self, ijson):
        company_id = ijson['company_id']
        doc_ids    = ijson['doc_ids']
        for doc_id in doc_ids:
            db_path = '/mnt/eMB_db/company_management/{0}/equality/{1}.db'.format(company_id, doc_id)
            conn = sqlite3.connect(db_path)
            cur  = conn.cursor()
            try:
                rcg_tab_xml_dct = self.read_rawdb(conn, cur)
                self.prep_insert_structure(conn, cur, rcg_tab_xml_dct)
            except Exception as e:
                print e
                print doc_id
                continue
            conn.close()
        return 'Done' 

if __name__ == '__main__':
    f_Obj = Formula_storage()
    # 35#30#38#46#28#54#57#48#62#56#52#53#26#36#29#59#49#32#39#40#43#58#60#5#27#31#45#55#37#33#42#47#50#41#34#51#44#1#3#2#4#61
    company_id = sys.argv[1]
    doc_ids    = sys.argv[2]
    ijson = {"company_id":company_id, "doc_ids":doc_ids.split('#')}
    print f_Obj.read_formulainfo(ijson)
    



