import os, sqlite3, json, hashlib, binascii, sets, sys
import db_utils
db_Obj = db_utils.Utils()
import utils.numbercleanup as numbercleanup
numbercleanup_obj   = numbercleanup.numbercleanup()
import report_year_sort
class Populate():

    def get_quid(self, text):
        m = hashlib.md5()
        m.update(text)
        quid = m.hexdigest()
        return quid

    def read_json_file(self, table_id, company_id):
        json_file_path = '/mnt/eMB_db/company_management/{0}/json_files/{1}.json'.format(company_id, table_id)
        if not os.path.exists(json_file_path):
            #print 'NOT EXISTS'
            return {}
        json_dct = {}
        with open(json_file_path, 'r') as j:
            json_dct = json.load(j)
        #if 'data' not in json_dct:
        #    tmpdd   = {}
        #    tmpdd['data']    = json_dct
        #    json_dct    = tmpdd
        return json_dct

    def read_table_data(self, company_id, table_id, txn_m={}, txn1_default={}, txn1={}):
        d,p,g = table_id.split('_')
        grid_json   = self.read_json_file(table_id, company_id)
        db_Obj.read_triplet_data(grid_json, table_id,txn_m, txn1_default, txn1)
        def create_cell_dict(rcs, ddict):
            rcs.sort(key=lambda r_c:(int(r_c.split('_')[0]), int(r_c.split('_')[1])))
            map_d   = {
                        'value' : 'GV',
                        'hch'   : 'HGH',
                        'vch'   : 'VGH',
                        'gh'    : 'GH',
                        
                        }
            txt_ar  = []
            x_ar    = []
            bbox    = []
            stype   = ''
            ph      = ''
            gphcsv   = {}
            for r_c in rcs:
                dd  = ddict[r_c]
                if stype    == '':
                    print [company_id, table_id, r_c]
                    stype   = map_d[dd['ldr']]
                txt_ar.append(dd['data'])
                chref   = dd.get('chref', '')
                if 0:#chref:
                    xml_ids =  map(lambda x:x+'@'+chref, filter(lambda x:x.strip(), dd['xml_ids'].split('$$')[:]))
                else:
                    xml_ids =  filter(lambda x:x.strip(), dd['xml_ids'].split('$$')[:])
                if dd['bbox']:
                    bbox += map(lambda x:map(lambda x1:int(x1), x.split('_')), dd['bbox'].split('$$'))
                x_ar.append(xml_ids)
                phcsv   = dd.get('phcsv', {})
                if ph == '' and phcsv:
                    gphcsv   = phcsv
                    pt  = phcsv.get('pt', '')
                    tp   = phcsv.get('p', '')
                    if tp and pt:
                        ph = '%s%s'%(pt,tp)
            vl = ' '.join(txt_ar)
            xm = '#'.join(map(lambda x: '#'.join(x), x_ar))
            try:
                clean_value = str(numbercleanup_obj.get_value_cleanup(vl))
            except:clean_value = ''
            dd = {'clean_value':clean_value, 'v':vl, 'x':xm, 'bbox':bbox, 'phcsv':gphcsv, 't':table_id, 'tt':table_id, 'd':d, 'p':p, 'g':g, 'ph':'', 'section_type':stype, 'ph':ph}           
            return dd
        for k, v in grid_json['data'].items():
            dd  = create_cell_dict([k], grid_json['data'])
            dd['r_c']   = k
            txn_m[(table_id, dd['x'])]  = dd
            pass   
        

    def read_triplet(self, table_id, txn1, xml, txn1_default, ignore_parent=None):
        triplet = {}
        flip    = txn1.get('FLIP_'+table_id)
        if not flip or flip == 'N':
            flip    = txn1_default.get('FLIP_'+table_id)
        if flip == 'NM':
            gv_tree = txn1_default.get(self.get_quid(table_id+'^!!^'+xml+'$:$NMERGE'))
        elif flip == 'M':
            gv_tree = txn1_default.get(self.get_quid(table_id+'^!!^'+xml+'$:$MERGE'))
        elif flip == 'Y':
            gv_tree = txn1_default.get(self.get_quid(table_id+'^!!^'+xml+'$:$FLIP'))
        else:
            gv_tree = txn1.get(self.get_quid(table_id+'^!!^'+xml))
        if not gv_tree:
            gv_tree = txn1_default.get(self.get_quid(table_id+'^!!^'+xml))
        if gv_tree:
            gv_tree = eval(gv_tree)
            triplet = gv_tree['triplet']
        return triplet

    def triple_process(self, triplet, txn_m, table_id):

        t_all_map_ref_text = {} 

        print ' triple_process: ', triplet.keys(), [table_id]
        vrp_info = triplet['VRP']


        pgh_text = []
        pgh_xml_ids = []
        pgh_bbox    = []
         
        gh_text = []
        gh_xml_ids = []
        gh_bbox     = []
         
        pvgh_text = []
        pvgh_xml_ids = []
        pvgh_bbox   = []

        vgh_text = []
        vgh_xml_ids = []
        vgh_bbox    = []


        hgh_text = []
        hgh_xml_ids = []
        hgh_bbox    = []

        for vrp_info_elm in vrp_info[:0]:
            pgh, gh, pvgh, vgh, hgh = vrp_info_elm
            for elm in pgh:
                pgh_text.append(elm[0])
                pgh_xml_ids.append(elm[1])  
                t_all_map_ref_text[str(elm[1])] = [ elm[0], 'GH' ]
                tk          = self.get_quid(table_id+'_'+elm[1])
                c_id        = txn_m.get('XMLID_MAP_'+tk)
                bbox    = []
                if c_id:
                    bbox    = eval(txn_m.get('BBOX_'+c_id, '[]')) #self.get_bbox_frm_xml(txn1, table_id, x)
                pgh_bbox.append(bbox)
                    
                
            for elm in gh:
                gh_text.append(elm[0])
                gh_xml_ids.append(elm[1])  
                t_all_map_ref_text[str(elm[1])] = [ elm[0], 'GH' ]
                tk          = self.get_quid(table_id+'_'+elm[1])
                c_id        = txn_m.get('XMLID_MAP_'+tk)
                bbox    = []
                if c_id:
                    bbox    = eval(txn_m.get('BBOX_'+c_id, '[]')) #self.get_bbox_frm_xml(txn1, table_id, x)
                gh_bbox.append(bbox)
             
            for elm in pvgh:
                #print 'pvgh: ', elm[1], elm[0]
                pvgh_text.append(elm[0])
                pvgh_xml_ids.append(elm[1])  
                tk          = self.get_quid(table_id+'_'+elm[1])
                c_id        = txn_m.get('XMLID_MAP_'+tk)
                bbox    = []
                if c_id:
                    bbox    = eval(txn_m.get('BBOX_'+c_id, '[]')) #self.get_bbox_frm_xml(txn1, table_id, x)
                pvgh_bbox.append(bbox)
             
            for elm in vgh:
                vgh_text.append(elm[0])
                vgh_xml_ids.append(elm[1])  
                t_all_map_ref_text[str(elm[1])] = [ elm[0], 'VGH' ]
                tk          = self.get_quid(table_id+'_'+elm[1])
                c_id        = txn_m.get('XMLID_MAP_'+tk)
                bbox    = []
                if c_id:
                    bbox    = eval(txn_m.get('BBOX_'+c_id, '[]')) #self.get_bbox_frm_xml(txn1, table_id, x)
                vgh_bbox.append(bbox)
             
            for elm in hgh:
                hgh_text.append(elm[0])
                hgh_xml_ids.append(elm[1])  
                t_all_map_ref_text[str(elm[1])] = [ elm[0], 'HGH' ]
                tk          = self.get_quid(table_id+'_'+elm[1])
                c_id        = txn_m.get('XMLID_MAP_'+tk)
                bbox    = []
                if c_id:
                    bbox    = eval(txn_m.get('BBOX_'+c_id, '[]')) #self.get_bbox_frm_xml(txn1, table_id, x)
                hgh_bbox.append(bbox)

        return pgh_text, pgh_xml_ids, gh_text, gh_xml_ids, pvgh_text, pvgh_xml_ids, vgh_text, vgh_xml_ids, hgh_text, hgh_xml_ids     , pgh_bbox, gh_bbox, pvgh_bbox, vgh_bbox, hgh_bbox, t_all_map_ref_text 
        

    def convert_xywh_to_xyx1y1(self, bboxs):
        tmpbbox = []
        for bbox in bboxs:
            if not isinstance(bbox, list):
                bbox    = [bbox]
            for tbox in bbox:
                x,y, w, h   = tbox
                tmpbbox.append([x, y, x+w, y+h])
        return tmpbbox
    def make_rawdb_row_data(self, row, col, groupid, doc_id, table_id, xml_id, txn_m, txn1_default, txn1, res_opr, table_cc, table_datatype):
        
        print [table_id, xml_id]
        triplet =  self.read_triplet(table_id, txn1, xml_id, txn1_default)
        pghtext, pghxmlid, ghtext, ghxmlid, pvghtext, pvghxmlid, vghtext, vghxmlid, hghtext, hghxmlid, pghbbox, ghbbox, pvghbbox, vghbbox, hghbbox , t_all_map_ref_text    = self.triple_process(triplet, txn_m, table_id)
        pghbbox = self.convert_xywh_to_xyx1y1(pghbbox)
        ghbbox  = self.convert_xywh_to_xyx1y1(ghbbox)
        pvghbbox = self.convert_xywh_to_xyx1y1(pvghbbox)
        vghbbox = self.convert_xywh_to_xyx1y1(vghbbox)
        hghbbox = self.convert_xywh_to_xyx1y1(hghbbox)
        

        taxo_level  = ''
        row_taxo_id = ''
        row_order   = row
        #row_col_groupid, row, col, groupid, docid, cellph, cellcsvc, cellcsvs, cellcsvv, celltype, pghtext, pghxmlid, pghbbox, ghtext, ghxmlid, ghbbox, pvghtext, pvghxmlid, pvghbbox, hghtext, hghxmlid, hghbbox, vghtext, vghxmlid, vghbbox, gvtext, gvxmlid, gvbbox, numval, pg, gridids, taxoid, srctype, valuetype, datatype, tl_type, direction, res_opr, comp_type, 
        row_col_groupid = '%s_%s_%s'%(row, col, groupid)
        dd      = txn_m[(table_id, xml_id)]
        phcsv   = dd['phcsv']
        cellph  = '%s%s'%(phcsv.get('pt', ''), phcsv.get('p', ''))
        cellcsvc    = [phcsv['c']]
        cellcsvs    = [phcsv['s']]
        cellcsvv    = [phcsv['vt']]
        gvtext      = [dd['v']]
        clean_value = dd['clean_value']
        numval      = clean_value
        datatype    = table_cc
        srctype     = ''
        taxo_id     = ''
        tl_type     = ''
        direction   = ''
        #res_opr     = ''
        comp_type   = 'G:C'
        pg          = table_id.split('_')[1]
        gridids     = '#'.join(table_id.split('_')) #+'#'+table_datatype
        celltype    = dd['r_c']
        gvxmlid     = [dd['x']]
        gvbbox      = [dd['bbox']]
        taxoid      = ''
        valuetype   = ''
        rtup    = (row_col_groupid, row, col, groupid, doc_id, cellph, cellcsvc, cellcsvs, cellcsvv, celltype, pghtext, pghxmlid, pghbbox, ghtext, ghxmlid, ghbbox, pvghtext, pvghxmlid, pvghbbox, hghtext, hghxmlid, hghbbox, vghtext, vghxmlid, vghbbox, gvtext, gvxmlid, gvbbox, numval, pg, gridids, taxoid, srctype, valuetype, datatype, tl_type, direction, res_opr, comp_type)
        tmpar   = []
        for i in rtup:
            if isinstance(i, list):
                i   = str(i)
            tmpar.append(i)    
        
        return tuple(tmpar)

    def connect_to_sqlite(self, db_path):
        import sqlite3
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        return conn, cur
        
        

    def populate_rawdb(self, company_id, doc_id, display_dict, table_cc=None, table_datatype=None, file_sufx=None):
        if not file_sufx:
            file_sufx   = ''
        if table_cc == None:
            table_cc    = 'Table6'
            table_datatype= 'TABLEAC'
        
        f_ar = []
        done_table      = {}
        txn_m           = {}
        txn1_default    = {}
        txn1            = {}
        db_path_org = '/mnt/eMB_db/company_management/{0}/equality/{1}.db'.format(company_id, doc_id)
        db_path = '/mnt/eMB_db/company_management/{0}/equality/{1}{2}.db'.format(company_id, doc_id, file_sufx)
        if not os.path.exists(db_path):
            os.system("cp %s %s"%(db_path_org, db_path))
        
        conn, cur   = self.connect_to_sqlite(db_path)
        sql         = "select max(groupid) from rawdb where datatype != '%s'" %(table_cc)
        cur.execute(sql)
        res         = cur.fetchone()
        conn.close()
        if res and res[0]:
            groupid     = int(res[0]) +1
        else:
            groupid     = 1
        f_i_ar      = []
        form_d  = {}
        dks = display_dict.keys()
        def sort_by_sig(dk):
            if 'LabelEqRoR' in dk:
                return 1
            elif 'LabelEq' in dk:
                return 0
            return 2
        dks.sort(key=lambda x:sort_by_sig(x))
        for dk in dks:
            vs  = display_dict[dk]
            
            ftype   = 'FORMULA G'
            fsign   = 'DIST'
            if 'nong' in dk.lower():
                ftype   = 'FORMULA NONG'
                fsign   = 'DIST'
            #print '\n===================================='
            
            for v in vs:
                if len(v)>=5:
                    fsign   = v[4]
                table_id, xml_id  = v[0][5].split('^')
                if table_cc == 'Table6':
                    #print '\nDK ', dk
                    #print v[0]
                    diff_table = 0
                    x_ar    = []
                    for e in v[1]:
                        #print '\t',e
                        x_ar.append(e[5])
                        tmptable_id, tmpxml_id  = e[5].split('^')
                        if table_id != tmptable_id:
                            diff_table = 1
                            #break
                    #print 'diff_table ', diff_table
                    #if diff_table == 0:continue
                    dup_flg = 0
                    x_ar_set    = sets.Set(x_ar)
                    for fx_ar in form_d.get(v[0][5], {}).get(len(x_ar), []):
                        if x_ar_set == fx_ar:
                            dup_flg = 1
                            continue
                    if dup_flg == 1:continue
                
                if table_id not in done_table:
                    self.read_table_data( company_id, table_id, txn_m, txn1_default, txn1)
                    done_table[table_id]    = 1
                if len(v[1]) == 0 and table_cc == 'Table6':
                    #print 'Empty operand ', dk, v
                    #llllllllllllllllllllllllllllllll
                    continue
                row = 1
                col = 1
                #rtup    = (row_col_groupid, row, col, groupid, doc_id, cellph, cellcsvc, cellcsvs, cellcsvv, celltype, pghtext, pghxmlid, pghbbox, ghtext, ghxmlid, ghbbox, pvghtext, pvghxmlid, pvghbbox, hghtext, hghxmlid, hghbbox, vghtext, vghxmlid, vghbbox, gvtext, gvxmlid, gvbbox, numval, pg, gridids, taxoid, srctype, valuetype, datatype, tl_type, direction, res_opr, comp_type)
                frm_ar  = []
                frm_v_ar  = []
                rtup    = self.make_rawdb_row_data(row, col, groupid, doc_id, table_id, xml_id, txn_m, txn1_default, txn1, 'R', table_cc, table_datatype)
                res_tup = ('=', (row, col))
                f_ar.append(rtup)
                #print '\nDK ', dk
                #print v[0]
                x_ar    = []
                for e in v[1]:
                    #print '\t',e
                    x_ar.append(e[5])
                    table_id, xml_id  = e[5].split('^')
                    if table_id not in done_table:
                        self.read_table_data( company_id, table_id, txn_m, txn1_default, txn1)
                        done_table[table_id]    = 1
                    row += 1
                    rtup    = self.make_rawdb_row_data(row, col, groupid, doc_id, table_id, xml_id, txn_m, txn1_default, txn1, 'O', table_cc, table_datatype)
                    frm_ar.append(('+', (row, col), '', ''))
                    frm_v_ar.append(((row, col), rtup[28]))
                    f_ar.append(rtup)
                form_d.setdefault(v[0][5], {}).setdefault(len(x_ar), []).append(sets.Set(x_ar))
                if len(frm_ar) >= 1:
                    #print (ftype, fsign)
                    if ftype == 'FORMULA NONG':
                        frm_ar  = fsign
                        fsign   = 'KPI'
                        if 'NonGEq2' in dk:
                            fsign   = 'Ratio'
                        elif 'NonGEq1' in dk:
                            fsign   = 'Percentage'
                        f_i_ar.append(('%s_%s'%(groupid, 1), str(res_tup),  str(frm_ar), str(frm_v_ar), groupid, ftype, fsign, dk))
                    #print (ftype, fsign)
                    else:
                        f_i_ar.append(('%s_%s'%(groupid, 1), str(res_tup),  str(frm_ar), str(frm_v_ar), groupid, ftype, fsign, dk))
                
                groupid  += 1
        #db_path = '/mnt/eMB_db/company_management/{0}/equality/{1}.db'.format(company_id, doc_id)
        conn, cur   = self.connect_to_sqlite(db_path)
        cols    = ['row_col_groupid', 'row', 'col', 'groupid', 'docid', 'cellph', 'cellcsvc', 'cellcsvs', 'cellcsvv', 'celltype', 'pghtext', 'pghxmlid', 'pghbbox', 'ghtext', 'ghxmlid', 'ghbbox', 'pvghtext', 'pvghxmlid', 'pvghbbox', 'hghtext', 'hghxmlid', 'hghbbox', 'vghtext', 'vghxmlid', 'vghbbox', 'gvtext', 'gvxmlid', 'gvbbox', 'numval', 'pg', 'gridids', 'taxoid', 'srctype', 'valuetype', 'datatype', 'tl_type', 'direction', 'res_opr', 'comp_type']
        #done_d  = {}
        #for r in f_ar:
        #    if r[0] in done_d:
        #        print 'EXISTS ', r
        #    print 'RR', r
        #    done_d[r[0]]    = 1
        print 'i_ar ', table_cc, len(f_ar)
        print 'f_i_ar ', table_cc, len(f_i_ar)
        sql = "delete from formulainfo where groupid in (select groupid from  rawdb where datatype='%s' or gridids like '%s')"%(table_cc, '%'+table_datatype)
        cur.execute(sql)
        sql = "delete from rawdb where datatype='%s' or  gridids like '%s'"%(table_cc, '%'+table_datatype)
        cur.execute(sql)
        cur.executemany("insert into rawdb(%s)values(%s)"%(', '.join(cols),  ', '.join(map(lambda x:'?', cols))), f_ar)
        try:
            cur.execute("alter table formulainfo add column formula_key text")
        except:pass

        cols    = ['fid', 'resultant_info', 'formula', 'opernands', 'groupid', 'formula_type', 'formula_sign', 'formula_key']
        cur.executemany("insert into formulainfo(%s)values(%s)"%(', '.join(cols),  ', '.join(map(lambda x:'?', cols))), f_i_ar)
        conn.commit()
        conn.close()

    def parse_table_data(self, table_id, company_id):
        grid_json   = self.read_json_file(table_id, company_id)
        r_c_d       = {}
        hch_c_cels  = {}
        hch_r_cels  = {}
        for k, v in grid_json['data'].items():
            r,c = map(lambda x:int(x), k.split('_'))
            v['xml_ids']    = '#'.join(v['xml_ids'].split('$$'))
            r_c_d[('RC', v['xml_ids'])] = k
            if v['ldr'] == 'hch':
                for c1 in range(int(v['colspan'])):
                    hch_c_cels.setdefault(c+c1, {})[k]    = 1
                for r1 in range(int(v['rowspan'])):
                    hch_r_cels.setdefault(r+r1, {})[k]  = 1
            elif v['ldr'] == 'value':
                r_c_d.setdefault(('R', r), {})[k]   = 1
                r_c_d.setdefault(('C', c), {})[k]   = 1
                r_c_d.setdefault(('GV', 'C'), {})[c]    = 1
                r_c_d.setdefault(('GV', 'R'), {})[r]    = 1
            
        grid_json['r_c_d']  = r_c_d
        for k, v in hch_c_cels.items():
            if len(v.keys()) == 1:
                r_c_d[('C', 'HGH', k)]  = 'H'
            else:
                r_c_d[('C', 'HGH', k)]  = 'D'
        for k, v in hch_r_cels.items():
            if len(v.keys()) == 1:
                r_c_d[('R', 'HGH', k)]  = 'V'
            else:
                r_c_d[('R', 'HGH', k)]  = 'D'
        return grid_json

    def update_traverse_path(self, ijson):
        company_id  = ijson['company_id']
        project_id  = ijson['project_id']
        db_path      = '/mnt/eMB_db/company_management/{0}/{1}/table_info.db'.format(company_id, project_id)
        print db_path
        conn, cur   = self.connect_to_sqlite(db_path)
        sql         = "select row_id, doc_id, page_no, grid_id, gv_rc, taxonomy_rc,  f_type, formula_id, equality_id, traverse_path, level_id, info_type, r_type, table_type, taxo_info from scoped_gv"
        cur.execute(sql)
        res         = cur.fetchall()
        conn.close()

        t_data  = {}
        done_table  = {}
        tble_d      = {}
        for r in res:
            row_id, doc_id, page_no, grid_id, gv_rc, taxonomy_rc,  f_type, formula_id, equality_id, traverse_path, level_id, info_type, r_type, tt, taxo_info  = r
            if traverse_path:
                t_data.setdefault(doc_id, {}).setdefault(traverse_path, {})[row_id]    = 1
        u_ar    = []
        for doc_id, tmpdata  in  t_data.items():
            db_path      = '/mnt/eMB_db/company_management/{0}/equality/{1}.db'.format(company_id, doc_id)
            conn, cur   = self.connect_to_sqlite(db_path)
            sql = "select eid, equality_type, equality_id, equality_index from equalityInfo"
            cur.execute(sql)
            res = cur.fetchall()
            tmpd    = {}
            for r in res:
                eid, equality_type, equality_id, equality_index = r
                tmpd[str(eid)]  = '%s_%s_%s'%(equality_type, equality_id, equality_index)
                
            for traverse_path_org, dd in tmpdata.items():
                traverse_path  = json.loads(binascii.a2b_hex(traverse_path_org))
                tmpar   = []
                u_flg   = 0
                for tr in traverse_path:
                    if 'expr_str' not in tr:
                        tr['expr_str']  = tmpd[str(tr['eq_id'])]
                        u_flg   = 1
                traverse_path   = binascii.b2a_hex(json.dumps(traverse_path))
                #print (traverse_path, traverse_path_org)
                if u_flg == 1:
                    u_ar.append((traverse_path, traverse_path_org))
        print 'u_ar', len(u_ar)
        db_path      = '/mnt/eMB_db/company_management/{0}/{1}/table_info.db'.format(company_id, project_id)
        conn, cur   = self.connect_to_sqlite(db_path)
        #cur.executemany('update scoped_gv set traverse_path=? where traverse_path=?', u_ar)
        #conn.commit()
        conn.close()

    def read_document_meta_data(self, company_id):
        inc_project_id  = '34'
        if str(company_id) not in {'1117':1}:
            inc_project_id  = company_id
        db_path = os.path.join('/mnt/eMB_db/company_management/{0}/'.format(company_id), 'document_info.db')
        conn        = sqlite3.connect(db_path, timeout=30000)
        cur         = conn.cursor()
        read_qry = """ SELECT doc_id, doc_name, doc_type, period_type, period, filing_type, meta_data FROM document_meta_info;  """        
        cur.execute(read_qry)
        t_data = cur.fetchall()
        conn.close()
        dd  = {}
        for row_data in t_data:
            doc_id, doc_name, doc_type, period_type, period, filing_type, meta_data = row_data
            try:
                meta_data   = json.loads(meta_data)
            except:
                meta_data   = {}
            ph  = '{0}{1}'.format(period_type, period)
            dd[str(doc_id)]  = {'ph':ph, 'ftype':filing_type, 'doc_type':doc_type, 'dn':doc_name, 'meta_data':meta_data, 'doc_rtype':meta_data.get("DocType", '')}
        return dd

    def read_tables(self, company_id, doc_id, tableid_list=[]):
        db_path = '/mnt/eMB_db/company_management/{0}/table_info.db'.format(company_id)
        con         = sqlite3.connect(db_path, timeout=30000)
        cur         = con.cursor()
        sql         = "select doc_id, page_no, grid_id from table_mgmt where doc_id=%s"%(doc_id)
        cur.execute(sql)
        res         = cur.fetchall()
        done_d  = {}
        for r in res:
            doc_id, page_no, grid_id    = r
            table_id    = '{0}_{1}_{2}'.format(doc_id, page_no, grid_id)    
            if tableid_list:
                if table_id not in tableid_list:continue
            if table_id in done_d:continue
            json_file   = '/mnt/eMB_db/company_management/{0}/json_files/{1}.json'.format(company_id, table_id)
            grid_data   = json.loads(open(json_file, 'r').read())
            done_d[table_id]    = (int(page_no), (int(grid_data['table_boundry'][1]), int(grid_data['table_boundry'][1])+int(grid_data['table_boundry'][3])))
        table_ids   = done_d.keys()
        table_ids.sort(key=lambda x:done_d[x])
        #for r in table_ids:
        #    print r, done_d[r]
        return table_ids


    def read_table_tagg(self, ijson):
        company_id  = ijson['company_id']
        project_id  = ijson['project_id']
        print ijson
        db_path      = '/mnt/eMB_db/company_management/{0}/{1}/table_info.db'.format(company_id, project_id)
        conn, cur   = self.connect_to_sqlite(db_path)
        sql         = "select row_id, table_id, classified_id, doc_id, page_no, grid_id, table_type, taxo_info from scoped_gv"
        cur.execute(sql)
        res         = cur.fetchall()
        tble_d  = {}
        consider_d  = {}
        super_key_d= {}
        super_key_doc= {}
        for r in res:
            row_id, table_id, classified_id, doc_id, page_no, grid_id, tt, taxo_info  = r
            if not taxo_info:
                taxo_info   = '[]'
            tble_d.setdefault(classified_id, {})[table_id]  = 1
            tble_d[table_id]    = {'row_id':str(row_id), 'type':tt, 'info':taxo_info, 'c_id':classified_id}
            if 'super_primary_key' in taxo_info:# and str(classified_id) != '-1':
                tag_d = self.form_table_tag_dict(taxo_info)
                #print tag_d
                if 'super_primary_key' in tag_d:
                    super_key_d[table_id]   = tag_d
                    super_key_doc.setdefault(table_id.split('_')[0], {})[table_id]  = 1
                
            if ijson.get('table_types') and str(classified_id) not in ijson['table_types']:continue
            if ijson.get('table_ids') and str(table_id) not in ijson['table_ids']:continue
            consider_d[classified_id]   = 1
        f_ar    = []
        super_keys  = super_key_d.keys()
        super_keys.sort()
        super_key   = ('', '')
        doc_mdata   = self.read_document_meta_data(company_id)
        if super_key_doc:
            type_d  = {}
            for k, v in doc_mdata.items():
                type_d.setdefault(v['doc_type'], {})[k] = 1
            for k, v in type_d.items():
                print k, v.keys()
            ph_order    = map(lambda x:doc_mdata[x]['ph'], doc_mdata.keys())
            ph_order    = report_year_sort.year_sort(ph_order)
            all_docs    = map(lambda x:str(x), doc_mdata.keys())
            all_docs.sort(key=lambda x:ph_order.index(doc_mdata[str(x)]['ph']), reverse=True)
            super_key_docs  = super_key_doc.keys()
            super_key_docs.sort(key=lambda x:all_docs.index(x))
            for doc in super_key_docs:
                table_ids   = self.read_tables(company_id, doc, super_key_doc[doc].keys())
                table_ids.reverse()
                super_key   = (table_ids[0], sorted(super_key_d[table_ids[0]]['super_primary_key'].keys()))
                break
                
                
        t_d   = {}
        csv_flg = 0
        for classified_id in consider_d.keys():
            print classified_id
            sql         = "select table_id from group_tables where classified_id=%s"%(classified_id)
            cur.execute(sql)
            tmpres  = cur.fetchall()
            table_d = tble_d[classified_id]
            for table_id, tinfo in table_d.items():
                tinfo   = tble_d[table_id]
                if table_id in  super_key_d:
                    tag_d   = super_key_d[table_id]
                else:
                    tag_d = self.form_table_tag_dict(tinfo['info'])
                #print tag_d.keys()
                primary_cols    = tag_d.get('primary_key', {}).keys()
                primary_cols    += tag_d.get('super_primary_key', {}).keys()
                range_cols      = tag_d.get('range', {}).keys()
                primary_rang_cols   = list(sets.Set(primary_cols).intersection(sets.Set(range_cols)))
                primary_cols.sort()
                primary_rang_cols.sort()
                dd  = (table_id,  'Type I' if (not tinfo['type']) else tinfo['type'], primary_cols, primary_rang_cols)
                t_d.setdefault(table_id.split('_')[0], []).append(dd)
            for table_id in tmpres:
                table_id    = table_id[0]
                if table_id in table_d:continue
                if table_id in  super_key_d:
                    tag_d   = super_key_d[table_id]
                else:
                    if table_id not in tble_d:
                        tag_d   = {}
                    else:    
                        tinfo   = tble_d[table_id]
                        tag_d = self.form_table_tag_dict(tinfo['info'])
                primary_cols    = tag_d.get('primary_key', {}).keys()
                primary_cols    += tag_d.get('super_primary_key', {}).keys()
                range_cols      = tag_d.get('range', {}).keys()
                primary_rang_cols   = list(sets.Set(primary_cols).intersection(sets.Set(range_cols)))
                primary_cols.sort()
                primary_rang_cols.sort()
                dd  = (table_id, 'Type I' if (not tinfo['type']) else tinfo['type'] , primary_cols, primary_rang_cols)
                t_d.setdefault(table_id.split('_')[0], []).append(dd)
                
        for k, v in t_d.items():
            if 'csv' in doc_mdata[k]['doc_type'].lower():
                csv_flg = 1
            f_ar.append(v)
        conn.close()

        return super_key, f_ar, csv_flg

    def form_table_tag_dict(self, info):
        info    = eval(info)
        if not info:
            return {}
        k_d = {}
        for r in info:
            cols    = {}
            for rc in r['v']:
                cols[int(rc.split('_')[1])] = 1
            k_d.setdefault(r['k'],  {}).update(cols)
        return k_d
                
        
            
            
                    
                
                    

    def read_traverse_path(self, ijson):
        company_id  = ijson['company_id']
        project_id  = ijson['project_id']
        db_path      = '/mnt/eMB_db/company_management/{0}/{1}/table_info.db'.format(company_id, project_id)
        conn, cur   = self.connect_to_sqlite(db_path)
        doc_ids     = ijson.get('doc_ids', [])
        if doc_ids:
            #sql         = "select row_id, doc_id, page_no, grid_id, gv_rc, taxonomy_rc,  f_type, formula_id, equality_id, traverse_path, level_id, info_type, r_type, table_type, taxo_info from scoped_gv where classified_id=%s and doc_id in (%s)"%(ijson['table_type'], ', '.join(map(lambda x:str(x), doc_ids)))
            sql         = "select row_id, classified_id, doc_id, page_no, grid_id, gv_rc, taxonomy_rc,  f_type, formula_id, equality_id, traverse_path, level_id, info_type, r_type, table_type, taxo_info from scoped_gv where  doc_id in (%s)"%( ', '.join(map(lambda x:str(x), doc_ids)))
        else:
            sql         = "select row_id, classified_id, doc_id, page_no, grid_id, gv_rc, taxonomy_rc,  f_type, formula_id, equality_id, traverse_path, level_id, info_type, r_type, table_type, taxo_info from scoped_gv"
        cur.execute(sql)
        res         = cur.fetchall()
        conn.close()

        t_data  = {}
        done_table  = {}
        tble_d      = {}
        if 0:
            consider_ttype  = {}
            for r in res:
                row_id, classified_id, doc_id, page_no, grid_id, gv_rc, taxonomy_rc,  f_type, formula_id, equality_id, traverse_path, level_id, info_type, r_type, tt, taxo_info  = r
                if info_type == 'table':
                    tid = '%s_%s_%s'%(doc_id, page_no, grid_id)
                    if tid in ijson['table_ids']:# ['4_54_1']: #['4_12_', '4_48_', '4_14_', '4_15_', '4_16_', '4_13_']:
                        consider_ttype[str(classified_id)]  = 1
            ijson['table_type'] = consider_ttype.keys()
            print ijson['table_type']
        
        for r in res:
            row_id, classified_id, doc_id, page_no, grid_id, gv_rc, taxonomy_rc,  f_type, formula_id, equality_id, traverse_path, level_id, info_type, r_type, tt, taxo_info  = r
            #if str(classified_id) in ['174', '116', '131']:continue
            try:
                xxx = binascii.a2b_hex(traverse_path)
            except:
                traverse_path   = ''
                pass
            if info_type == 'table':
                if not traverse_path:
                    traverse_path   = 'EMPTY_PATH'
                if taxo_info:
                    taxo_info   = eval(taxo_info)
                else:
                    taxo_info   = []
                tid = '%s_%d_%d'%(doc_id, page_no, grid_id)
                #if tid in ['6_29_4']:continue
                if (not ijson.get('table_types') or str(classified_id) in ijson['table_types']) and ( not ijson.get('table_ids') or tid in ijson['table_ids']) and (not ijson.get('row_ids') or (str(row_id) in ijson['row_ids'])):
                    t_data.setdefault(doc_id, {}).setdefault(traverse_path, {})[tid]    = {'row_id':str(row_id), 'type':tt, 'info':taxo_info, 'c_id':classified_id}
                if tid not in tble_d:
                    tble_d[tid]      = {'type':tt, 'info':taxo_info, 'c_id':classified_id}
                elif taxo_info and (not tble_d[tid]['info']):
                    tble_d[tid]      = {'type':tt, 'info':taxo_info, 'c_id':classified_id}
                    
        f_ar    = []
        t_dict  = {}
        f_individual_table  = []
        error_d = {}
        ind_d   = {}
        ind_indv_d   = {}
        for doc_id, tmpdata  in  t_data.items():
            print doc_id #, tmpdata
            db_path      = '/mnt/eMB_db/company_management/{0}/equality/{1}.db'.format(company_id, doc_id)
            conn, cur   = self.connect_to_sqlite(db_path)
            try:
                cur.execute('alter table formulainfo add column formula_key text')
            except:pass
            for traverse_path, dd in tmpdata.items():
                #print traverse_path, dd 
                rids = {}
                for tid, tmpdd in dd.items():
                    rids[tmpdd['row_id']]   = 1
                if traverse_path == 'EMPTY_PATH':
                    for table_id in dd.keys():
                        u_info  = {}
                        rids = {}
                        rids[str(dd[table_id]['row_id'])]   = 1
                        for tid in [table_id]:
                            tid = '_'.join(tid.split('#'))
                            if tid not in t_dict:
                                grid_json   = self.parse_table_data(tid, company_id)
                                t_dict[tid]  = grid_json
                            t_info  = tble_d.get(tid, {'info':[], 'type':''})
                            info_ar = []
                            print [tid, t_info.get('c_id')]
                            e_flg1  = 0
                            if t_info['type'] == 'Type I':
                                info_ar = self.form_type_I_info(tid, t_info, t_dict)
                            else:
                                info_ar = self.form_type_II_info(tid, t_info, t_dict)
                                if not info_ar and t_info['type'] == 'Type II':
                                    e_flg1  = 1
                            e_flg, tmpinfo_ar  = self.group_by_cells(info_ar)
                            tmpinfo_ar.sort(key=lambda x:tuple(map(lambda x1:(int(t_dict[tid]['r_c_d'][('RC', x1)].split('_')[0]), int(t_dict[tid]['r_c_d'][('RC', x1)].split('_')[1])), x['v'])))
                            if e_flg == 1  or e_flg1:
                                error_d[tid]    = 1
                            t_info['info']  = tmpinfo_ar
                            if (info_ar or t_info['type']):
                                u_info[tid] = t_info
            
                            
                        tmptup  = [company_id, doc_id, (table_id,'', '', '', ''), '', '', {'usertag':u_info}]
                        ind_indv_d[len(f_individual_table)]   = rids.keys()
                        f_individual_table.append([tmptup])
                    continue
                traverse_path  = json.loads(binascii.a2b_hex(traverse_path))
                tmpar   = []
                if isinstance(traverse_path, dict):
                    print 'INN', traverse_path
                    tmpar   = self.form_rule_from_table_lets(traverse_path, company_id, t_dict, tble_d, doc_id, error_d)
                    ind_indv_d[len(f_individual_table)]   = rids.keys()
                    f_individual_table.append(tmpar)
                    continue
                else:
                    for tr in traverse_path:
                        fid1 = tr['f1'].strip('F-')
                        fid2 = tr['f2'].strip('F-')
                        print tr
                        #print [fid1, fid2]
                        f1key   = tr['f1key']
                        f2key   = tr['f2key']
                        t1  = '_'.join(tr['t'].split('#'))
                        t2  = '_'.join(tr['t2'].split('#'))
                        if not t1:continue
                        if not t2:continue
                        #print '1'
                        u_info  = {}
                        if 'expr_str' not in tr:continue
                        if tr['t'] not in tr['tinfo']:
                            tr['tinfo'][tr['t']]    = 1
                        if tr['t2'] not in tr['tinfo']:
                            tr['tinfo'][tr['t2']]    = 1
                        #print 'DD '
                        if not tr['tinfo']:continue
                        if len(tr['tinfo'].keys()) <=1:continue
                        
                        for tid in tr['tinfo'].keys():
                            tid = '_'.join(tid.split('#'))
                            if tid not in t_dict:
                                grid_json   = self.parse_table_data(tid, company_id)
                                t_dict[tid]  = grid_json
                            t_info  = tble_d.get(tid, {'info':[], 'type':''})
                            info_ar = []
                            e_flg1  = 0
                            if t_info['type'] == 'Type I':
                                info_ar = self.form_type_I_info(tid, t_info, t_dict)
                            else:
                                info_ar = self.form_type_II_info(tid, t_info, t_dict)
                                if not info_ar and t_info['type'] == 'Type II':
                                    e_flg1  = 1
                            e_flg, tmpinfo_ar  = self.group_by_cells(info_ar)
                            tmpinfo_ar.sort(key=lambda x:tuple(map(lambda x1:(int(t_dict[tid]['r_c_d'][('RC', x1)].split('_')[0]), int(t_dict[tid]['r_c_d'][('RC', x1)].split('_')[1])), x['v'])))
                            #print
                            #for x in tmpinfo_ar:
                            #    print '\n========================================='
                            #    print x 
                            #    print tuple(map(lambda x1:(int(t_dict[tid]['r_c_d'][('RC', x1)].split('_')[0]), int(t_dict[tid]['r_c_d'][('RC', x1)].split('_')[1])), x['v']))
                            if e_flg  or e_flg1:
                                error_d[tid]    = 4
                            t_info['info']  = tmpinfo_ar
                            if (info_ar or t_info['type']):
                                u_info[tid] = t_info
            
                            
                        rc1  = tr['rc']
                        rc2  = tr['rc2']
                        if rc1 not in t_dict[t1]['data']:
                            error_d[t1] = 5
                            continue
                        if rc2 not in t_dict[t2]['data']:
                            error_d[t2] = 6
                            continue
                        x1  = t_dict[t1]['data'][rc1]['xml_ids']
                        x2  = t_dict[t2]['data'][rc2]['xml_ids']
                        f1key   = self.convert_kpi(f1key)
                        f2key   = self.convert_kpi(f2key)
                        if f2key and f1key == '':
                            tmptup  = [company_id, doc_id, (t2, '%s^%s'%(t2, x2), t1, '%s^%s'%(t1, x1), tr['expr_str']), f2key, f1key, {'usertag':u_info}]
                        else:
                            tmptup  = [company_id, doc_id, (t1, '%s^%s'%(t1, x1), t2, '%s^%s'%(t2, x2), tr['expr_str']), f1key, f2key, {'usertag':u_info}]
                        tmpar.append(tmptup)
                        
                        pass
                if  tmpar:
                    ind_d[len(f_ar)]   = rids.keys()
                    f_ar.append(tmpar)
        if ijson.get('gen_rule') == 'Y':
            tmp_f_ar    = []
            for ii, r in  enumerate(f_ar):
                tmp_f_ar.append((ind_d[ii], r))
            f_ar    = tmp_f_ar
            tmp_f_ar    = []
            for ii, r in enumerate(f_individual_table):
                tmp_f_ar.append((ind_indv_d[ii], r))
            f_individual_table    = tmp_f_ar
            return f_ar, f_individual_table
        if error_d:
            print 'Error : table error ', error_d
            xxxxxxxxxxxxxxxxxxxxxxxxx
        if ijson.get('return_rule') == 'Y':
            return f_ar, f_individual_table, error_d
        print len(f_ar)
        path    = '/var/www/html/muthu/traverse_path'
        if ijson.get('table_ids'):
            fname_al    = '%s'%('^'.join(ijson['table_ids']))
            fname_ind   = '%s'%('^'.join(ijson['table_ids']))
        else:
            fname_al    = 'ALL'
            fname_ind   = 'individual_table'
        print 'Mul', len(f_ar), f_ar
        print 'IND',  len(f_individual_table)
        if f_ar:
            f1  = open('%s/%s.txt'%(path, fname_al), 'w')
            print '%s/%s.txt'%(path, fname_al)
            f1.write(json.dumps(f_ar))
            f1.close()
        if f_individual_table:
            f1  = open('%s/%s.txt'%(path, fname_ind), 'w')
            print '%s/%s.txt'%(path, fname_ind)
            f1.write(json.dumps(f_individual_table))
            f1.close()
        print 'Error ', error_d, map(lambda x:tble_d.get(x, {}).get('c_id'), error_d.keys())
        return
        path    = '/var/www/html/muthu/traverse_path'
        f1  = open('%s/individual_table.txt'%(path), 'w')
        f1.write(json.dumps(f_individual_table))
        f1.close()
        print 'http://172.16.20.229/muthu/traverse_path/all_tpaths.txt'
        print 'http://172.16.20.229/muthu/traverse_path/individual_table.txt'
        return
        #os.system('rm -rf '+path)
        os.system('mkdir -p '+path)
        for i, rr in enumerate(f_ar):
            f1  = open('%s/%s.txt'%(path, i), 'w')
            f1.write(json.dumps([rr]))
            f1.close()
        path    = '/var/www/html/muthu/individual_table'
        os.system('rm -rf '+path)
        os.system('mkdir -p '+path)
        for i, rr in enumerate(f_individual_table):
            f1  = open('%s/%s.txt'%(path, i), 'w')
            f1.write(json.dumps([rr]))
            f1.close()
        print 'http://172.16.20.229/muthu/traverse_path/'
        print 'http://172.16.20.229/muthu/individual_table/'
        print 'Error ', error_d

        return [], []

    def form_rule_from_table_lets(self, traverse_path, company_id, t_dict, tble_d, doc_id, error_d):
        tmpar   = []
        for tr in [traverse_path]:
            fid1 = tr['fid'].strip('F-')
            f1key   = tr['f1key']
            t1  = '_'.join(tr['t'].split('#'))
            if not t1:continue
            u_info  = {}
            if tr['t'] not in tr['tinfo']:
                tr['tinfo'][tr['t']]    = 1
            for tid in tr['tinfo'].keys():
                tid = '_'.join(tid.split('#'))
                if tid not in t_dict:
                    grid_json   = self.parse_table_data(tid, company_id)
                    t_dict[tid]  = grid_json
                t_info  = tble_d.get(tid, {'info':[], 'type':''})
                print [tid, t_info.get('c_id'),  t_info['type']]
                info_ar = []
                e_flg1  = 0
                if t_info['type'] == 'Type I':
                    info_ar = self.form_type_I_info(tid, t_info, t_dict)
                else:
                    info_ar = self.form_type_II_info(tid, t_info, t_dict)
                    if not info_ar and t_info['type'] == 'Type II':
                        e_flg1  = 1
                e_flg, tmpinfo_ar  = self.group_by_cells(info_ar)
            
                tmpinfo_ar.sort(key=lambda x:tuple(map(lambda x1:(int(t_dict[tid]['r_c_d'][('RC', x1)].split('_')[0]), int(t_dict[tid]['r_c_d'][('RC', x1)].split('_')[1])), x['v'])))
                if e_flg or e_flg1:
                    error_d[tid]    = 2
                t_info['info']  = tmpinfo_ar
                if (info_ar or t_info['type']):
                    u_info[tid] = t_info

                
            rc1  = tr['rc']
            #print t1
            if rc1 not in t_dict[t1]['data']:
                error_d[t1] = 3
                continue
            
            x1  = t_dict[t1]['data'][rc1]['xml_ids']
            f1key   = self.convert_kpi(f1key)
            tmptup  = [company_id, doc_id, (t1, '%s^%s'%(t1, x1), '', '', ''),  f1key, '', {'usertag':u_info}]
            tmpar.append(tmptup)
        return tmpar


    def convert_kpi(self, f1key):
        if f1key.split(':')[0].split('_')[-1] == 'KPI':
            tmpf1key    = f1key.split(':')
            if 'NonGEq2' in tmpf1key[0]:
                tmpf1key[0] = '_'.join(tmpf1key[0].split('_')[:-1]+['Ratio'])
            elif 'NonGEq1' in tmpf1key[0]:
                tmpf1key[0] = '_'.join(tmpf1key[0].split('_')[:-1]+['Percentage'])
            f1key    = ':'.join(tmpf1key)
        return f1key
            

    def group_by_cells(self, info_ar):
        n_ar    = []
        done_d  = {}
        e_flg   = 0
        for i, r in  enumerate(info_ar):
            m_ks    = {}
            if i in done_d:continue
            s1  = sets.Set(r['v'])
            if isinstance(r['k'], list):
                for k1 in r['k']:
                    m_ks[k1]   = 1
            else:    
                m_ks[r['k']]   = 1
            for j in range(i+1, len(info_ar)):
                s2  = sets.Set(info_ar[j]['v'])
                if s1 == s2:
                    done_d[j]  = 1
                    m_ks[info_ar[j]['k']]   = 1
            if ('group' in m_ks) and ('range' in m_ks):
                e_flg   = 1
            r['k']  = m_ks.keys()
            n_ar.append(r)
        return e_flg, n_ar

    def form_type_I_info_all(self, tid, t_info, t_dict):
        r_c_d   = t_dict[tid]['r_c_d']
        cols    = r_c_d.get(('GV', 'C'), {}).keys()
        cols.sort()
        tmpv    = []
        for c in cols:
            tmpv.append({'k':'', 'v':list(r_c_d[('C', c)].keys()), 'vh':'D', 'order_v':'V'})
                    
        for tmpr in tmpv:
            tmpr['v'].sort(key=lambda x:(int(x.split('_')[0]), int(x.split('_')[1])))
            tmpr['v']  = map(lambda x:t_dict[tid]['data'][x]['xml_ids'], filter(lambda x:t_dict[tid]['data'][x]['ldr'] == 'value', tmpr['v']))
        return tmpv
            

    def form_type_I_info(self, tid, t_info, t_dict):
        r_c_d   = t_dict[tid]['r_c_d']
        cols    = r_c_d.get(('GV', 'C'), {}).keys()
        cols.sort()
        tmpv    = []
        for c in cols:
            tmpv.append({'k':'attributes', 'v':list(r_c_d[('C', c)].keys()), 'vh':'D', 'order_v':'V'})
                    
        for tmpr in tmpv:
            tmpr['v'].sort(key=lambda x:(int(x.split('_')[0]), int(x.split('_')[1])))
            tmpr['v']  = map(lambda x:t_dict[tid]['data'][x]['xml_ids'], filter(lambda x:t_dict[tid]['data'][x]['ldr'] == 'value', tmpr['v']))
        return tmpv
            
        

    def form_type_II_info(self, tid, t_info, t_dict):
        r_c_d   = t_dict[tid]['r_c_d']
        info_ar = []
        for r in t_info['info']:
            r_d     = {}
            c_d     = {}
            tmpar   = []
            for c_id in r['v']:
                if 'x' in c_id:
                    x   = '#'.join(c_id.split('$$'))
                    c_id    = r_c_d[('RC', x)]
                if c_id not in t_dict[tid]['data']:
                    return []
                tmpar.append(c_id)
            r['v']  = tmpar
                
            for c_id in r['v']:
                row, col    = map(lambda x:int(x), c_id.split('_'))
                if t_dict[tid]['data'][c_id]['ldr'] == 'value':
                    c_d.setdefault(col, {})[c_id]   = 1
            tmpv    = []
            done_c_ids  = {}
            for c, c_ids in c_d.items():
                if sets.Set(r_c_d[('C', c)].keys()) == sets.Set(c_ids.keys()):
                    vh_flg  = r_c_d[('C', 'HGH', c)]
                    tmpv.append({'k':r['k'], 'v':c_ids.keys(), 'vh':vh_flg, 'order_v':'V'})
                    done_c_ids.update(c_ids)
            nids    = sets.Set(r['v']) - sets.Set(done_c_ids)
            for c_id in list(nids):
                row, col    = map(lambda x:int(x), c_id.split('_'))
                if t_dict[tid]['data'][c_id]['ldr'] == 'value':
                    r_d.setdefault(row, {})[c_id]   = 1
            for tr, c_ids in r_d.items():
                if sets.Set(r_c_d[('R', c)].keys()) == sets.Set(c_ids.keys()):
                    vh_flg  = r_c_d[('R', 'HGH', tr)]
                    tmpv.append({'k':r['k'], 'v':c_ids.keys(), 'vh':vh_flg, 'order_v':'R'})
                    done_c_ids.update(c_ids)
            nids    = sets.Set(filter(lambda x: t_dict[tid]['data'][x]['ldr'] == 'value', r['v'])) - sets.Set(done_c_ids)
            if nids:
                tmpv.append({'k':r['k'], 'v':list(nids), 'vh':'D', 'order_v':'D'})
                    
            for tmpr in tmpv:
                tmpr['v'].sort(key=lambda x:(int(x.split('_')[0]), int(x.split('_')[1])))
                tmpr['v']  = map(lambda x:t_dict[tid]['data'][x]['xml_ids'], filter(lambda x:t_dict[tid]['data'][x]['ldr'] == 'value', tmpr['v']))
                tmpr['v']  = tmpr['v'][:2]
            info_ar += tmpv
        return info_ar

    def read_taxo_info(self, company_id, table_ids):
        f_d = {}
        for tid in table_ids:
            t_dict  = {}
            grid_json   = self.parse_table_data(tid, company_id)
            t_dict[tid]  = grid_json
            info_ar = self.form_type_I_info_all(tid, {}, t_dict)
            e_flg, tmpinfo_ar  = self.group_by_cells(info_ar)
            tmpinfo_ar.sort(key=lambda x:tuple(map(lambda x1:(int(t_dict[tid]['r_c_d'][('RC', x1)].split('_')[0]), int(t_dict[tid]['r_c_d'][('RC', x1)].split('_')[1])), x['v'])))
            f_d[tid]    = {'info':tmpinfo_ar}
        return f_d
        
        



if __name__ == '__main__':
    obj = Populate()
    res = obj.read_table_tagg({'company_id':sys.argv[1], 'project_id':5, 'table_types':sys.argv[2].split('#'), 'table_ids':[], 'row_ids':[]})
    #res = obj.read_taxo_info(1604, ['4_3_8'])
    print res
    #res = obj.read_traverse_path({'company_id':1604, 'project_id':5, 'table_type':[], 'table_ids':['2_12_7.txt']})
    #res = obj.read_traverse_path({'company_id':1604, 'project_id':5, 'table_type':['94']})
    #res = obj.update_traverse_path({'company_id':1604, 'project_id':5, 'table_type':81})
    #print json.dumps(res)
