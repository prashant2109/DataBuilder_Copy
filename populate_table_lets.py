import os, sys, json, copy, sets, lmdb, hashlib
import data_builder.db_data as db_data
db_dataobj  = db_data.PYAPI()
import pyapi
m_obj   = pyapi.PYAPI()
import utils.numbercleanup as numbercleanup
numbercleanup_obj   = numbercleanup.numbercleanup()
import convert_nonascii_ascii as convert_nonascii_ascii
conobj = convert_nonascii_ascii.convert()
import report_year_sort
import perm_comb

perm_obj = perm_comb.perm_comb()

import db.get_conn as get_conn
conn_obj    = get_conn.DB()

import time

import shelve

from igraph  import *

class Validate():
    def parse_ijson(self, ijson):
        company_name    = ijson['company_name']
        mnumber         = ijson['model_number']
        model_number    = mnumber
        deal_id         = ijson['deal_id']
        project_id      = ijson['project_id']
        company_id      = "%s_%s"%(project_id, deal_id)
        projtype = 'fe' 
        if 'template_id' in ijson:
            projtype = ijson['template_id'] 
        return company_name, model_number, deal_id, project_id, company_id, projtype

    def get_quid(self, text):
        m = hashlib.md5()
        m.update(text)
        quid = m.hexdigest()
        return quid

    def get_lookup_file(self):
        ddict = {'QSTR':['three months ended', 'three months', '3 months', '3-months'], 'PERSHARE':['per share', 'per-share'], 'PeriodQ':['Q1', 'Q2', 'Q3', 'Q4'], 'PERCENTAGE':['%'] , 'RATIO':['Ratio']}
        return ddict

    def convert_unichar_to_html_entities(self, text):
        try:
            t1 = text.decode('utf-8')
            t1 = t1.encode('ascii', 'xmlcharrefreplace')
            return t1
        except:
            try:
                t2 = text.decode('iso-8859-1')
                t2 = t2.encode('ascii', 'xmlcharrefreplace')
                return t2
            except:
                t2 = text.encode('ascii', 'xmlcharrefreplace')
                return t2



    def get_file_scoped_data(self, gpath , scope_label):
        # These are preset SCOPES
        graphml_files = os.listdir(gpath)
        file_ddict = {}
        if scope_label in ['COL-RAW', 'ROW-RAW', 'RAW']:
           for graph_file in graphml_files:
               graph_file = str(graph_file)
               if 'FORMULA' in graph_file: continue
               if '_RAW' in graph_file:
                   level_name = graph_file.split('_')[0]  
                   if 'ROWCOL' in graph_file:
                      if level_name not in file_ddict:
                         file_ddict[level_name] = {} 
                      file_ddict[level_name]['ROWCOL'] = graph_file  
                   else:
                      if level_name not in file_ddict:
                         file_ddict[level_name] = {} 
                      file_ddict[level_name]['DATA'] = graph_file  

        elif scope_label in ['COL-RES', 'ROW-RES', 'RES']:
           for graph_file in graphml_files:
               graph_file = str(graph_file)
               if 'FORMULA' in graph_file: continue
               if '_RES' in graph_file:
                   level_name = graph_file.split('_')[0]  
                   if 'ROWCOL' in graph_file:
                      if level_name not in file_ddict:
                         file_ddict[level_name] = {} 
                      file_ddict[level_name]['ROWCOL'] = graph_file  
                   else:
                      if level_name not in file_ddict:
                         file_ddict[level_name] = {} 
                      file_ddict[level_name]['DATA'] = graph_file  

        elif scope_label in ['COL-REP', 'ROW-REP', 'REP']:
           for graph_file in graphml_files:
               graph_file = str(graph_file)
               if 'FORMULA' in graph_file: continue
               if '_REP' in graph_file:
                   level_name = graph_file.split('_')[0]  
                   if 'ROWCOL' in graph_file:
                      if level_name not in file_ddict:
                         file_ddict[level_name] = {} 
                      file_ddict[level_name]['ROWCOL'] = graph_file  
                   else:
                      if level_name not in file_ddict:
                         file_ddict[level_name] = {} 
                      file_ddict[level_name]['DATA'] = graph_file  


        elif scope_label in ['FORMULA-REP']:
           for graph_file in graphml_files:
               graph_file = str(graph_file)
               if 'FORMULA' not in graph_file: continue
               if '_REP' in graph_file:
                   level_name = graph_file.split('_')[0]  
                   if 'ROWCOL' in graph_file:
                      if level_name not in file_ddict:
                         file_ddict[level_name] = {} 
                      file_ddict[level_name]['ROWCOL'] = graph_file  
                   else:
                      if level_name not in file_ddict:
                         file_ddict[level_name] = {} 
                      file_ddict[level_name]['DATA'] = graph_file  


        elif scope_label in ['FORMULA-RES']:
           for graph_file in graphml_files:
               graph_file = str(graph_file)
               if 'FORMULA' not in graph_file: continue
               if '_RES' in graph_file:
                   level_name = graph_file.split('_')[0]  
                   if 'ROWCOL' in graph_file:
                      if level_name not in file_ddict:
                         file_ddict[level_name] = {} 
                      file_ddict[level_name]['ROWCOL'] = graph_file  
                   else:
                      if level_name not in file_ddict:
                         file_ddict[level_name] = {} 
                      file_ddict[level_name]['DATA'] = graph_file  


        elif scope_label in ['FORMULA-RAW']:
           for graph_file in graphml_files:
               graph_file = str(graph_file)
               if 'FORMULA' not in graph_file: continue
               if '_RAW' in graph_file:
                   level_name = graph_file.split('_')[0]  
                   if 'ROWCOL' in graph_file:
                      if level_name not in file_ddict:
                         file_ddict[level_name] = {} 
                      file_ddict[level_name]['ROWCOL'] = graph_file  
                   else:
                      if level_name not in file_ddict:
                         file_ddict[level_name] = {} 
                      file_ddict[level_name]['DATA'] = graph_file  

        elif scope_label in ['DOCWISEDOCPH-REP' ]:
           for graph_file in graphml_files:
               graph_file = str(graph_file)
               if 'FORMULA' in graph_file: continue
               if '_REP' in graph_file:
                   level_name = graph_file.split('_')[0]  
                   if 'ROWCOL' in graph_file:
                      if level_name not in file_ddict:
                         file_ddict[level_name] = {} 
                      file_ddict[level_name]['ROWCOL'] = graph_file  
                   else:
                      if level_name not in file_ddict:
                         file_ddict[level_name] = {} 
                      file_ddict[level_name]['DATA'] = graph_file  
           file_ddict['DOCINFO'] =  'DOCINFO.graphml' 

        elif scope_label in ['DOCWISEDOCPH-RES' ]:
           for graph_file in graphml_files:
               graph_file = str(graph_file)
               if 'FORMULA' in graph_file: continue
               if '_RES' in graph_file:
                   level_name = graph_file.split('_')[0]  
                   if 'ROWCOL' in graph_file:
                      if level_name not in file_ddict:
                         file_ddict[level_name] = {} 
                      file_ddict[level_name]['ROWCOL'] = graph_file  
                   else:
                      if level_name not in file_ddict:
                         file_ddict[level_name] = {} 
                      file_ddict[level_name]['DATA'] = graph_file  
           file_ddict['DOCINFO'] =  'DOCINFO.graphml' 

        elif scope_label in ['DOCWISEDOCPH-RAW' ]:
           for graph_file in graphml_files:
               graph_file = str(graph_file)
               if 'FORMULA' in graph_file: continue
               if '_RES' in graph_file:
                   level_name = graph_file.split('_')[0]  
                   if 'ROWCOL' in graph_file:
                      if level_name not in file_ddict:
                         file_ddict[level_name] = {} 
                      file_ddict[level_name]['ROWCOL'] = graph_file  
                   else:
                      if level_name not in file_ddict:
                         file_ddict[level_name] = {} 
                      file_ddict[level_name]['DATA'] = graph_file  
           file_ddict['DOCINFO'] =  'DOCINFO.graphml' 
        return file_ddict 
          


    def getformuladata(self, gpath, formula_filename):
        mandatory_elms = [ 'clean_value', 'gv_text']
         
        G = Graph.Read_GraphML(gpath+formula_filename)
        all_formula_vs = G.vs.select(lambda v:v["type"]=="FORMULA") #graph.vs.select(lambda vertex: vertex.index % 2 == 1)

         
        fname_lookup_dict = {}   
        all_formulas = [] 
        for formula_node in all_formula_vs:
            #print ' == ',formula_node  
            fid = G.vs[formula_node.index]["ID"]
            if not str(fid).strip(): continue
            opr_res_vs = G.vs.select(lambda v:v["fid"]==fid)
            res_elms = []
            opr_elms = []
            for opr in opr_res_vs:
                       if str(opr['res_flag']) == '1': 
                          fname = opr['filenames']
                          hashkey = opr['ID']    
                          colid = opr['colid']
                          res_elms.append((fname, hashkey, colid, opr['chksum']))  # operands
                          if fname not in fname_lookup_dict:
                             fname_lookup_dict[fname] = [] 
                          fname_lookup_dict[fname].append(hashkey+'#'+str(colid))
                       else: 
                          fname = opr['filenames']
                          hashkey = opr['ID']    
                          colid = opr['colid']
                          opr_elms.append((fname, hashkey, colid)) # resultants
                          if fname not in fname_lookup_dict:
                             fname_lookup_dict[fname] = [] 
                          fname_lookup_dict[fname].append(hashkey+'#'+str(colid))
            all_formulas.append((fid, res_elms, opr_elms)) 


        data_fname_dict = {}  
        for fname, all_hashkey_cols in fname_lookup_dict.items():
            data_fname_dict[fname] = {}
            Gdata = Graph.Read_GraphML(gpath+fname)
            all_hashkey_cols = list(set(all_hashkey_cols))  
            shortlisted_vs = Gdata.vs.select(lambda v:v["hashkeycol"] in all_hashkey_cols)
            for vs in shortlisted_vs:
                ddict = { 'fname':fname, 'hashkey':vs['hashkey'], 'col':vs['col'], 'clean_value':vs['clean_value'], 'gv_text':vs['gv_text'] , 'hashkeycol':vs['hashkeycol'], 'rep_flg':vs['rep_flg'], 'res_flg':vs['res_flg'] }
                #print ddict  
                data_fname_dict[fname][vs['hashkeycol']] = copy.deepcopy(ddict)
                  
        
        data_formulas = []
        for (fid, res_elms, opr_elms) in all_formulas:
            data_ar = [ fid, [], [] ] 
            for res_elm in res_elms:
                fname = res_elm[0]
                hashkey = res_elm[1]
                colid = str(res_elm[2])
                check_sum = res_elm[3]
                data_ar[1].append( data_fname_dict[fname][hashkey+'#'+str(colid)] )
                data_ar[1][-1]['chksum'] = check_sum                
            for res_elm in opr_elms:
                fname = res_elm[0]
                hashkey = res_elm[1]
                colid = str(res_elm[2])
                data_ar[2].append( data_fname_dict[fname][hashkey+'#'+str(colid)] )             
            data_formulas.append(data_ar[:])
        return data_formulas  
 



    def getdata_formula(self, gpath, formula_filename,  parameter, mandatory_elms):

        #print gpath
        #print gpath+formula_filename
        #print gpath+data_file
        #sys.exit()

        norm_parameter = self.normalise_parameters([ parameter ])
        normal_flg = 1
        if 'FORMULA:' in norm_parameter[0]:
           normal_flg = 0
           formula_part = norm_parameter[0].split(':')[1]
           parameter = norm_parameter[0].split(':')[2]
           #print formula_part
           #print parameter
           #sys.exit() 
        else: 
           parameter = norm_parameter[0]

        G = Graph.Read_GraphML(gpath+formula_filename)
        all_formula_vs = G.vs.select(lambda v:v["type"]=="FORMULA") #graph.vs.select(lambda vertex: vertex.index % 2 == 1)

       
        all_hashkey_cols_dict = {}
        all_ddict = {}

          
        for formula_node in all_formula_vs:
            #print 
            #print '=========================================' 
            #print ' P: ', formula_node.index 
            fid = G.vs[formula_node.index]["ID"]
            if not str(fid).strip(): continue
            opr_res_vs = G.vs.select(lambda v:v["fid"]==fid) #graph.vs.select(lambda vertex: vertex.index % 2 == 1)

            if normal_flg:
                for opr in opr_res_vs:
                    hashkey = opr['ID']
                    colid = opr['colid']
                    fname = opr['filenames']
                    if fname not in all_hashkey_cols_dict:
                       all_hashkey_cols_dict[fname] = {}

                    if hashkey+'#'+str(colid) not in all_hashkey_cols_dict[fname]:
                       all_hashkey_cols_dict[fname][hashkey+'#'+str(colid)] = []
                    all_hashkey_cols_dict[fname][hashkey+'#'+str(colid)].append(opr['fid'])
            else:
                oper_elms = []
                formed_keys = []
                for opr in opr_res_vs:
                    
                    if formula_part == 'RESULTANT':
                       if str(opr['res_flag']) == '1': pass
                       else: 
                          fname = opr['filenames']
                          hashkey = opr['ID']    
                          colid = opr['colid']
                          oper_elms.append((fname, hashkey, colid))  # operands
                          continue 
                    elif formula_part == 'OPERANDS':
                       if str(opr['res_flag']) != '1': pass
                       else: 
                          fname = opr['filenames']
                          hashkey = opr['ID']    
                          colid = opr['colid']
                          oper_elms.append((fname, hashkey, colid)) # resultants
                          continue 
                    else:
                       print 'TODO - Avinash'
                       sys.exit() 
                    hashkey = opr['ID']
                    colid = opr['colid']
                    fname = opr['filenames']
                    if fname not in all_hashkey_cols_dict:
                       all_hashkey_cols_dict[fname] = {}

                    if hashkey+'#'+str(colid) not in all_hashkey_cols_dict[fname]:
                       all_hashkey_cols_dict[fname][hashkey+'#'+str(colid)] = [  ]
                    all_hashkey_cols_dict[fname][hashkey+'#'+str(colid)].append(opr['fid'])

                    #all_hashkey_cols_dict[fname][hashkey+'#'+str(colid)] = []
                    #formed_keys.append((fname, hashkey+'#'+str(colid)))
                    
        del G

        if parameter == 'checksum':

            print 'checksum todo '      
            sys.exit()
        else:
            for data_file, hashkey_cols_dict in all_hashkey_cols_dict.items():
                all_hashkey_cols = hashkey_cols_dict.keys() 
                all_ddict[data_file] = {}
                Gdata = Graph.Read_GraphML(gpath+data_file)
                all_hashkey_cols = list(set(all_hashkey_cols))  
                shortlisted_vs = Gdata.vs.select(lambda v:v["hashkeycol"] in all_hashkey_cols)
                for elm in shortlisted_vs:
                    ddict = {} 
                    ddict[parameter]= elm[parameter] 
                    keyelm = elm[parameter]              
                    for mandatory_elm in mandatory_elms:
                        ddict[mandatory_elm] = elm[mandatory_elm]
                    if keyelm not in all_ddict[data_file]:
                       all_ddict[data_file][keyelm] = [] 
                    all_ddict[data_file][keyelm].append(copy.deepcopy(ddict))
                del Gdata
          
        return all_ddict, parameter , all_hashkey_cols_dict
      



    def getdata_taxohkey(self, gpath, data_file,  key, val, key_parameter,  selc_parameters):
           ddict = {}
           G = Graph.Read_GraphML(gpath+data_file)
           shortlisted_vs = G.vs.select(lambda v:v[key]==val) #graph.vs.select(lambda vertex: vertex.index % 2 == 1)
           for elm in shortlisted_vs:
               all_col = elm[key_parameter]
               my_info = {}
               for sprm in selc_parameters:
                   my_info[sprm] = elm[sprm]
                   
               if all_col not in ddict:
                  ddict[all_col] =  []
               ddict[all_col].append(copy.deepcopy(my_info))
           del G 
           return ddict    

    def getdata_rowcol(self, gpath, data_file, rowcol_file, rowcol, selc_parameters):
            #print ' selc_parameters: ', selc_parameters   
            #print 'file: ', gpath+data_file
            if rowcol == 'COL':
               #print ' col getdata_rowcol: ', gpath+rowcol_file
               #print ' rowcol: ', gpath+rowcol_file
               G = Graph.Read_GraphML(gpath+rowcol_file)
               all_cols = G.vs["col"]
               all_cols = filter(lambda x:x!='', all_cols[:])

               del G
               G = Graph.Read_GraphML(gpath+data_file)
               ddict = {}
               for all_col in all_cols:
                   shortlisted_vs = G.vs.select(lambda v:v["linehashkeycol"]==all_col) #graph.vs.select(lambda vertex: vertex.index % 2 == 1) # "v_linehashkeycol"
                   short_ar = [] 
                   for elm in shortlisted_vs:
                       my_info = {}
                       for sprm in selc_parameters:
                           my_info[sprm] = elm[sprm]
                       short_ar.append(copy.deepcopy(my_info))
                   ddict[all_col] = short_ar[:]
               del G  

            if rowcol == 'ROW':
               #print ' row getdata_rowcol: ', gpath+rowcol_file
               G = Graph.Read_GraphML(gpath+rowcol_file)
               all_rows = G.vs["row"]
               all_rows = filter(lambda x:x!='', all_rows[:])  # hashkeys -> unique row id
               del G
               G = Graph.Read_GraphML(gpath+data_file)
               ddict = {}
               for all_row in all_rows:
                   shortlisted_vs = G.vs.select(lambda v:v["hashkey"]==all_row) #graph.vs.select(lambda vertex: vertex.index % 2 == 1)
                    
                   short_ar = [] 
                   for elm in shortlisted_vs:
                       my_info = {}
                       #print selc_parameters , elm
                       for sprm in selc_parameters:
                           my_info[sprm] = elm[sprm]
                       short_ar.append(copy.deepcopy(my_info))
                   ddict[all_row] = short_ar[:]
               del G  
            return ddict    

   
    def normalise_parameters(self, parameters):
        ar = []
        for parameter in parameters:
            if parameter in [ 'hashkey', 'col', 'line_hashkey', 'abs_hashkey', 'clean_value', 'row', 'gv_text', 'docph']:
               ar.append(parameter)

            elif parameter == 'CellPH':
               ar.append('cellph')

            elif parameter == 'CellPHPeriod':
               ar.append('cellphptype')
            
            elif parameter == 'Scale':
               ar.append('cellcsvs')

            elif parameter == 'Currency':
               ar.append('cellcsvc')

            elif parameter == 'ValueType':
               ar.append('cellcsvv')

            elif parameter == 'GVText':
               ar.append('gv_text')
            elif parameter == 'VCHText':
               ar.append('vgh_text')

            elif parameter == 'GVSign':
               ar.append('signchange')

            elif parameter == 'CellRep':
               ar.append('rep_flg')

            elif parameter == 'CellRes':
               ar.append('res_flg')

            elif parameter == 'ResultantCellDerived':
               ar.append( 'FORMULA:RESULTANT:derived'  )

            elif parameter == 'OperandFormulaCellPH':
               ar.append( 'FORMULA:OPERANDS:cellph'  )

            elif parameter == 'ResultantFormulaCellPH':
               ar.append( 'FORMULA:RESULTANT:cellph'  )

            elif parameter == 'ChecksumFormula':
               ar.append( 'FORMULA:RESULTANT:checksum'  )

            
            elif parameter == 'DocPH':
               ar.append( 'docph'  )
            else:
               print 'TO DO - Avinash Parameter'
               print parameter
               sys.exit() 
        return ar      

    def handle_parameter(self, data_dict, parameter):
        ar = self.normalise_parameters([ parameter ])       
        parameter = ar[0]
        #print data_dict
        new_data_dict = {}
        for k , vs in data_dict.items():
            new_data_dict[k] = {}
            for v in vs:
                m = v[parameter]
                if m not in new_data_dict[k]:
                   new_data_dict[k][m] = []
                new_data_dict[k][m].append(v)
            
        return new_data_dict   , parameter  


    def get_hashkey_filemap(self,  gpath):
        mandatory_elms = [ 'hashkey', 'col', 'line_hashkey', 'abs_hashkey']
        graphml_files = os.listdir(gpath)

        data_files = {}
        for graph_file in graphml_files:
            graph_file = str(graph_file)
            if 'FORMULA' in graph_file: continue
            if (('_RAW' in graph_file) or ('_RES' in graph_file) or ('_REP' in graph_file)):
               d = str(graph_file.split('_')[0])
               if ('RAW' in graph_file):

                   if 'RAW' not in data_files:
                      data_files['RAW'] = {}  
                   if d not in data_files['RAW']:
                      data_files['RAW'][d] = {}                     
                   if 'ROWCOL' in graph_file:
                      data_files['RAW'][d]['ROWCOL'] = graph_file
                   else:          
                      data_files['RAW'][d]['DATA'] = graph_file
               
               if ('RES' in graph_file):

                   if 'RES' not in data_files:
                      data_files['RES'] = {}  
                   if d not in data_files['RES']:
                      data_files['RES'][d] = {}                     

                   if 'ROWCOL' in graph_file:
                      data_files['RES'][d]['ROWCOL'] = graph_file
                   else:          
                      data_files['RES'][d]['DATA'] = graph_file
                      
               if ('REP' in graph_file):
                  
                   if 'REP' not in data_files:
                      data_files['REP'] = {}  
                   if d not in data_files['REP']:
                      data_files['REP'][d] = {}                     
                     
                   if 'ROWCOL' in graph_file:
                      data_files['REP'][d]['ROWCOL'] = graph_file
                   else:          
                      data_files['REP'][d]['DATA'] = graph_file
              
        filedict = { }
        for graph_file in graphml_files:
            graph_file = str(graph_file)
            if (('_RAW.' in graph_file) or ('_RES.' in graph_file) or ('_REP.' in graph_file)):
               if 'FORMULA' in graph_file: continue
               if 'RAW' in graph_file:
                  if 'RAW' not in filedict:  
                     filedict['RAW'] = {}
                  fileflg = 'RAW'
               if 'RES' in graph_file:
                  if 'RES' not in filedict:  
                     filedict['RES'] = {}
                  fileflg = 'RES'
               if 'REP' in graph_file:
                  if 'REP' not in filedict:  
                     filedict['REP'] = {}
                  fileflg = 'REP'

               G = Graph.Read_GraphML(gpath+graph_file)
               hashkeys = G.vs["hashkey"]
               for hashkey in hashkeys:
                   filedict[fileflg][hashkey] = data_files[fileflg][graph_file.split('_')[0]] 
               del G

        return filedict 
  


    def initiate_graph_results_num_to_label(self,  gpath, results_path, number_label_expression, number_label_dexpression, label_number_expression):
        mandatory_elms = [ 'hashkey', 'col', 'line_hashkey', 'abs_hashkey']
        graphml_files = os.listdir(gpath)
        try:
             os.system('rm -rf '+results_path)
        except:
             pass  
        os.system('mkdir -p '+results_path)

        #print graphml_files
        #sys.exit()
        for graph_file in graphml_files:
            graph_file = str(graph_file)
            if (('_RAW.' in graph_file) or ('_RES.' in graph_file) or ('_REP.' in graph_file)):
               if 'FORMULA' in graph_file: continue
               level_name = graph_file.split('_')[0]  
               #print ' == ', level_name, ' ++ ', graph_file
               G = Graph.Read_GraphML(gpath+graph_file)
               print ' == ', level_name, ' ++ ', gpath+graph_file
               no_vertices = len(G.vs)
               hashkeys = []
               cols = []
               rows = []   
               line_hashkeys = []
               abs_hashkeys = []
               hashcolkeys = []
               table_types = []
               group_ids = []
               row_taxo_ids = []
               valdict = {}
               table_row_cols = []
               gv_xml_ids = []  
               for v in G.vs:
                   #print dir(v)
                   #print v 
                   hashkeys.append(v["hashkey"])
                   cols.append(v["col"])
                   line_hashkeys.append(v["line_hashkey"])
                   abs_hashkeys.append(v["abs_hashkey"])
                   hashcolkeys.append(v["hashkey"]+'#'+str(int(v["col"])))   
                   table_types.append(v["table_types"])
                   group_ids.append(v["group_ids"])
                   row_taxo_ids.append(v["row_taxo_id"])
                   rows.append(v["row"])
                      
                   gv_xml_ids.append(v["gv_xml_ids"])  
                   #table_row_cols.append(v["table_row_col"]) 
                   for mydict in [ number_label_expression, number_label_dexpression, label_number_expression]:
                       for exp, exp_dict in mydict.items():
                           for sub_exp, exps in exp_dict.items():
                               for i, sexp in enumerate(exps):
                                   mykey = exp+'_'+sub_exp+'_'+str(i)
                                   if mykey not in valdict:
                                      valdict[mykey] = []
                                   valdict[mykey].append("")           
 
               del G  

               newG = Graph()
               newG.add_vertices( no_vertices  )
               newG.to_directed(False)
               newG.vs["hashkey"] = hashkeys
               newG.vs["col"] = cols
               newG.vs["line_hashkey"] = line_hashkeys
               newG.vs["abs_hashkey"] = abs_hashkeys
               newG.vs["hashcolkeys"] = hashcolkeys
               newG.vs["table_types"] = table_types 
               newG.vs["group_ids"] = group_ids
               newG.vs["row_taxo_ids"] = row_taxo_ids
               newG.vs["row"] = rows
               #newG.vs["table_row_cols"] = table_row_cols[:]    
               newG.vs["gv_xml_ids"] = gv_xml_ids 

               for k, ar in valdict.items():
                   newG.vs[k] = ar    
               
               print 'Initiate number to label: ', results_path+graph_file
               newG.write_graphml(results_path+graph_file)
               del newG     

         




    def initiate_graph_results(self,  gpath, logical_expression, logical_nodedep_expression, num_analytical_expression, validation_expressions, opr_result_expression):
        mandatory_elms = [ 'hashkey', 'col', 'line_hashkey', 'abs_hashkey']
        graphml_files = os.listdir(gpath)
        results_path = gpath+'lbvresults/'
        try:
             os.system('rm -rf '+results_path)
        except:
             pass  
        os.system('mkdir -p '+results_path)

        sh = shelve.open(results_path+'/rules_shelve.sh', 'n')
        dd = {} 
        dd['logical_expression'] = logical_expression
        dd['logical_nodedep_expression'] = logical_nodedep_expression
        dd['num_analytical_expression'] = num_analytical_expression
        dd['validation_expressions'] = validation_expressions
        dd['opr_result_expression'] = opr_result_expression
        sh['data'] = dd 
        sh.close() 
         
        for graph_file in graphml_files:
            graph_file = str(graph_file)
            if (('_RAW.' in graph_file) or ('_RES.' in graph_file) or ('_REP.' in graph_file)):
               if 'FORMULA' in graph_file: continue
               level_name = graph_file.split('_')[0]  
               #print ' == ', level_name, ' ++ ', graph_file
               G = Graph.Read_GraphML(gpath+graph_file)
               print ' == ', level_name, ' ++ ', gpath+graph_file
               no_vertices = len(G.vs)
               hashkeys = []
               cols = []
               line_hashkeys = []
               abs_hashkeys = []
               hashcolkeys = []
               table_types = []
               group_ids = []
               row_taxo_ids = []
               for v in G.vs:
                   #print dir(v)
                   #print v 
                   hashkeys.append(v["hashkey"])
                   cols.append(v["col"])
                   line_hashkeys.append(v["line_hashkey"])
                   abs_hashkeys.append(v["abs_hashkey"])
                   hashcolkeys.append(v["hashkey"]+'#'+str(int(v["col"])))   
                   table_types.append(v["table_types"])
                   group_ids.append(v["group_ids"])
                   row_taxo_ids.append(v["row_taxo_id"]) 
               del G  

               newG = Graph()
               newG.add_vertices( no_vertices  )
               newG.to_directed(False)
               newG.vs["hashkey"] = hashkeys
               newG.vs["col"] = cols
               newG.vs["line_hashkey"] = line_hashkeys
               newG.vs["abs_hashkey"] = abs_hashkeys
               newG.vs["hashcolkeys"] = hashcolkeys
               newG.vs["table_types"] = table_types 
               newG.vs["group_ids"] = group_ids
               newG.vs["row_taxo_ids"] = row_taxo_ids
               
               print 'Initiate Results Store: ', results_path+graph_file
               newG.write_graphml(results_path+graph_file)
               del newG     
               #sys.exit() 
        #sys.exit()   



    def compute_distinct_count(self, new_data_dict):
        count_dict = {}  
        for plevel, para_ddict in new_data_dict.items():
            count_dict[plevel] = len(para_ddict.keys())
        return count_dict  

    def compute_all_count(self, new_data_dict):
        count_dict = {}  
        for plevel, para_ddict in new_data_dict.items():
            count_dict[plevel] = 0
            for p, vs in para_ddict.items(): 
                count_dict[plevel] += len(vs)
        return count_dict  

    
    def compute_condition_count(self, count_dict, condition_expr):
        condition_expr = condition_expr.strip()
        #print 'condition_expr: ', condition_expr
        condition_expr_sp = condition_expr.split()
        if len(condition_expr_sp) == 2:
           # GT 1 , LT 1
           if (condition_expr_sp[0] == 'GT'):    
               d = int(condition_expr_sp[1])
               ar = []
               for t, v in count_dict.items():
                   if v > d:
                      ar.append(t)
               return ar
                 
           if (condition_expr_sp[0] == 'GTEQ'):    
               d = int(condition_expr_sp[1])
               ar = []
               for t, v in count_dict.items():
                   if v >= d:
                      ar.append(t)
               return ar
                
           elif (condition_expr_sp[0] == 'LT'):    
               d = int(condition_expr_sp[1])
               ar = []
               for t, v in count_dict.items():
                   if v < d:
                      ar.append(t)
               return ar
               
           elif (condition_expr_sp[0] == 'LTEQ'):    
               d = int(condition_expr_sp[1])
               ar = []
               for t, v in count_dict.items():
                   if v <= d:
                      ar.append(t)
               return ar

           elif (condition_expr_sp[0] == 'EQ'):    
               d = int(condition_expr_sp[1])
               ar = []
               for t, v in count_dict.items():
                   if v == d:
                      ar.append(t)
               return ar

           elif (condition_expr_sp[0] == 'NEQ'):    
               d = int(condition_expr_sp[1])
              
               ar = []
               for t, v in count_dict.items():
                   if v != d:
                      ar.append(t)
               return ar
           else:
               print 'UNK operator - ', condition_expr 
               sys.exit()  
               
        else:
               print 'UNK operator - 2', condition_expr 
               sys.exit()  
                

    def perform_lookup(self, lookup_elms, elm):
        for lookup_elm in lookup_elms:
            if lookup_elm in elm:
               return 1
        return 0 


    def inc_dec_parameter_based(self, norm_parameter, value, d):

        #if value:
        #print ' inc_dec_parameter_based value: ', value 
        if norm_parameter in ['cellph']:
           return value[:2]+str(int(value[2:])+d)
        else:
           print 'TODO inc_dec_parameter_based ' 
           sys.exit()
                            

    def compute_condition_set(self, new_data_dict, expr_condition, gpath):

         if 'REMAINDER-FROM-DOCINFOPH' in expr_condition:
            docgpath = gpath+'DOCINFO.graphml'
            G = Graph.Read_GraphML(docgpath)
            all_phs = G.vs["doc_ph"]
            del G  
            new_data_dict_new = {} 
            for plevel, vsdict in new_data_dict.items():
                keys = vsdict.keys()
                sli = list(set(all_phs) - set(keys))
                if sli:
                   new_data_dict_new[plevel] = {}
                   for k in keys:
                       new_data_dict_new[plevel][k] = vsdict[k]
            return new_data_dict_new            
         else:
            print ' To DO - compute_condition_set'
            sys.exit()        


    def spike_score_elm(self, d1, d2, expr_operator, expr_f):
        if d1['gv_text'] == '': return '', ''
        
        if d2['gv_text'] == '': return '', ''

        try: 
            n1 = float(d1['clean_value'])
            n2 = float(d2['clean_value'])
        except:
            return '', ''

        if expr_operator == 'GTINC':
           if n2 >= n1:
              return abs(n2 - n1) > expr_f, 1
        if expr_operator == 'LTINC':
           if n2 >= n1:
              return abs(n2 - n1) < expr_f, 1
        if expr_operator == 'GTDEC':
           if n2 < n1:
              return abs(n1 - n2) > expr_f, 1
        if expr_operator == 'LTDEC':
           if n2 < n1:
              return abs(n1 - n2) < expr_f, 1
        if expr_operator == 'ABSGT':
           if n2 != n1:
              return abs(n1 - n2) > expr_f, 1
        if expr_operator == 'ABSLT':
           if n2 != n1:
              return abs(n1 - n2) < expr_f, 1
        return '', ''                           

    def handle_posspike_expression(self, new_data_dict, expr,  gpath):
        # GTINC, LTINC, GTDEC, LTDEC, ABSP

        expr_condition = expr['elmcondition']

        expr_operator = expr_condition.split()[0]
        expr_f = float(expr_condition.split()[1])
 
        #print [ expr_operator, expr_f ]
        res = {} 
        for k, vsdict in new_data_dict.items():
              for vk, vs in vsdict.items():
                  n_vs = []
                  for v in vs:
                      n_vs.append((int(v['col']), v))
                  n_vs.sort()
                  new_vs = map(lambda x:x[1], n_vs[:])

                  for i in range(1, len(new_vs)):
                      #print '%%%%%%%%%%%%%%%%%%%%%%%'
                      diff_status, diff_flg = self.spike_score_elm(new_vs[i-1], new_vs[i], expr_operator, expr_f) # prev , curr
                      if diff_status:
                         if k not in res:
                            res[k] = {}
                         if vk not in res[k]:
                            res[k][vk] = []
                         res[k][vk].append(new_vs[i])
        return res                


    def handle_onoff_expression(self, new_data_dict, expr,  gpath):

        res = {} 
        for k, vsdict in new_data_dict.items():
              for vk, vs in vsdict.items():
                  n_vs = []
                  for v in vs:
                      n_vs.append((int(v['col']), v))
                  n_vs.sort()
                  new_vs = map(lambda x:x[1], n_vs[:])
                  on_off_ar = {} 
                  for v in new_vs:
                      if v['gv_text'] == '':
                         if 0 not in on_off_ar:
                            on_off_ar[0] = []
                         on_off_ar[0].append(v)
                      else: 
                         if 1 not in on_off_ar:
                            on_off_ar[1] = []
                         on_off_ar[1].append(v)
                         
                  if len(on_off_ar.keys()) == 1: continue
                        
                  if k not in res:
                     res[k] = {}
                  if vk not in res[k]:
                     res[k][vk] =  on_off_ar[0]
        return res                

   

    def handle_foreach_astrix_incdec(self, new_data_dict, expr, year_len, norm_parameter, gpath):
        print 'norm_parameter: ', norm_parameter  , gpath
        if 'NOTIN-DOCINFOPH' in expr['elmcondition']:
           docgpath = gpath+'DOCINFO.graphml'
           G = Graph.Read_GraphML(docgpath)
           new_data_dict_new = {} 
           for plevel, vsdict in new_data_dict.items():
               for k, vs in vsdict.items():
                   if not k.strip(): continue  
                   ret_val = self.inc_dec_parameter_based( norm_parameter, k, year_len)
                   shortlisted_vs = G.vs.select(lambda v:v["doc_ph"]==ret_val)             
                   if len(shortlisted_vs) == 0:
                      if plevel not in new_data_dict_new:
                         new_data_dict_new[plevel] = {}
                      new_data_dict_new[plevel][k] = vs
           del G 
           return new_data_dict_new  

        elif 'GREATERTHAN-DOCINFOPH' in expr['elmcondition']:
           import report_year_sort
           docgpath = gpath+'DOCINFO.graphml'
           G = Graph.Read_GraphML(docgpath)
           new_data_dict_new = {} 
           for plevel, vsdict in new_data_dict.items():
               for k, vs in vsdict.items():
                   if not k.strip(): continue  
                   ret_val = self.inc_dec_parameter_based( norm_parameter, k, year_len)
                   for v in vs:
                       #print v
                       doc_ph = v["docph"]
                       if not doc_ph: continue
                       if (ret_val == doc_ph): continue  
                       sort_year = report_year_sort.year_sort([ret_val, doc_ph])   

                       i1 = sort_year.index(ret_val)
                       i2 = sort_year.index(doc_ph)
                       if i1 > i2:  
                          if plevel not in new_data_dict_new:
                             new_data_dict_new[plevel] = {}
                          new_data_dict_new[plevel][k] = vs
                          #print ' GREATERTHAN-DOCINFOPH', ret_val,  ' DOCPH: ', doc_ph
                          #print sort_year 
                          #sys.exit()  

                   '''  
                   shortlisted_vs = G.vs.select(lambda v:v["doc_ph"]==ret_val)             
                   if len(shortlisted_vs) == 0:
                      if plevel not in new_data_dict_new:
                         new_data_dict_new[plevel] = {}
                      new_data_dict_new[plevel][k] = vs
                   '''  
           del G 
           return new_data_dict_new  

        else:
           print ' handle_foreach_astrix_incdec: '
           sys.exit() 

    def handle_foreach_astrix(self, new_data_dict, expr_condition, lookup_ddict):
        results = {}
        if 'CONTAINS:L1' in expr_condition:
           lookup_key = expr_condition.split('#')[1]
           print 'LOOK UP REQUIRED:', lookup_key 
           lookup_elms = lookup_ddict[lookup_key]  
           for plevel, vsdict in new_data_dict.items():
               for k, vs in vsdict.items():
                   if not k.strip(): continue
                   flg = self.perform_lookup(lookup_elms, k) 
                   if flg:
                      if plevel not in results:
                         results[plevel] = {}
                      if k not in results[plevel]:
                         results[plevel][k] = vs[:]
           return results 
       
        elif 'EQ ' == expr_condition[:3]:
            eq_str = expr_condition[3:].strip()
            if eq_str == '""':
               eq_str = ''
            for plevel, vsdict in new_data_dict.items():
                for k, vs in vsdict.items():
                    if str(k) != str(eq_str):  continue
                    if plevel not in results:
                       results[plevel] = {}
                    if k not in results[plevel]:
                       results[plevel][k] = vs[:]
            return results

        elif 'NEQ ' == expr_condition[:4]:
            eq_str = expr_condition[4:].strip()
            if eq_str == '""':
               eq_str = ''
            for plevel, vsdict in new_data_dict.items():
                for k, vs in vsdict.items():
                    if str(k) == str(eq_str):  continue
                    if plevel not in results:
                       results[plevel] = {}
                    if k not in results[plevel]:
                       results[plevel][k] = vs[:]
            return results 
        else:

            print expr_condition 
            print 'handle_foreach_astrix - TODO'
            sys.exit()  


    def handle_filter_operator_condition(self, new_data_dict, expr, lookup_ddict, norm_parameter, gpath):
                       print '       filtervalue handling' , expr  
                       #preprocessing_data[mykey] = copy.deepcopy(new_data_dict)  
                       if expr['filtervalue'] == 'DISTINCT':
                          if (expr['gpoperator'] == 'FOREACH'):
                              print "for each plevel we have to write for loop and apply elmcondition 1"
                              sys.exit()
                          elif (expr['gpoperator'] == 'COUNT'):
                              #print "for each plevel publish and apply elmcondition 2"
                              count_dict = self.compute_distinct_count(new_data_dict)
                              res = self.compute_condition_count(count_dict, expr['elmcondition'])
                              new_res = {}
                              for e in res:
                                  new_res[e] = new_data_dict[e]   
                              return new_res
                          elif (expr['gpoperator'] == 'SET'):
                              res = self.compute_condition_set(new_data_dict, expr['elmcondition'], gpath)
                              return res 
                          else:
                              print expr
                              print 'TODO- Avinash - handle_filter_operator_condition 1'
                              sys.exit()   
                           
                       elif expr['filtervalue'] == '*':
                          if (expr['gpoperator'] == 'FOREACH'):
                              print " --- for each plevel we have to write for loop and apply elmcondition 3"
                              #print expr                     
                              res = self.handle_foreach_astrix(new_data_dict, expr['elmcondition'], lookup_ddict)
                              return res
                          elif (expr['gpoperator'] == 'COUNT'):
                              #print "for each plevel publish and apply elmcondition 4"
                              count_dict = compute_all_count(new_data_dict)  # this may not be in use
                              res = self.compute_condition_count(count_dict, expr['elmcondition'])
                              new_res = {}
                              for e in res:
                                  new_res[e] = new_data_dict[e]   
                              return new_res
                          elif ('FOREACH:+' in expr['gpoperator']) or ('FOREACH:-' in expr['gpoperator']):
                              year_len = 0
                              if '+' in expr['gpoperator']:
                                 d = int(expr['gpoperator'].split('+')[1])
                                 year_len = 0 + d
                              elif '-' in expr['gpoperator']:
                                 d = int(expr['gpoperator'].split('-')[1])
                                 year_len = 0 - d
                              res = self.handle_foreach_astrix_incdec(new_data_dict, expr, year_len, norm_parameter, gpath)
                              return res  
                          elif ('ON-OFF' in expr['gpoperator']):
                              #print '**ON-OFF**'
                              #print new_data_dict['b9ef7de978fe20e61815230507d1da08'].keys()
                              #print expr
                              res = self.handle_onoff_expression(new_data_dict, expr, gpath)
                              return res
                          elif ('POS-SPIKE' in expr['gpoperator']):
                              #print '**POS-SPIKE**'
                              res = self.handle_posspike_expression(new_data_dict, expr, gpath)
                              return res
                          else:
                              print expr
                              print 'TODO- Avinash - handle_filter_operator_condition 2'
                              sys.exit()   
                       else:
                             print 'There is not other filtervalue type 5'
                             sys.exit()     
 

    

    def handle_formula_scope(self, expr_ar, pre_process_data, mandatory_elms, scope_file_dict, gpath, lookup_ddict, exp_id):
        
        print 'handle_formula_scope'
        final_result_dict = {} 
        expr_parameters = list(set(mandatory_elms[:])) 
        for expr_row, expr in enumerate(expr_ar):
            #print '       -- expr: ', expr                
            scope = expr['scope'] 
            file_name_dict = {}
            if 'FORMULA' in scope:
               #print 'FORMULA ---> ' , scope
               print ' FORMULAS: ', scope_file_dict[scope]
               #formula_filename = scope_file_dict[scope]


               for k, vsdict in scope_file_dict[scope].items():
                   #print ' -- ', k, ' ++ ', vsdict 
                   #print expr 
                   data_dict, norm_parameter, all_hashkey_cols_dict = self.getdata_formula(gpath, vsdict['DATA'], expr['parameter'], mandatory_elms)
                   #print data_dict, norm_parameter 

                    
                   for fname, ddict in data_dict.items():
                       new_data_dict = {'t':ddict} 
                       results_dict  = self.handle_filter_operator_condition(new_data_dict, expr, lookup_ddict, norm_parameter, gpath)
                       for k, vsdict in results_dict.items():
                           for ph,  vals in vsdict.items():
                               for val in vals:
                                   res_key = []
                                   for mandatory_elm in mandatory_elms:
                                       res_key.append(val[mandatory_elm])
                                   res_tup = tuple(res_key) 
                                   if fname not in final_result_dict:
                                      final_result_dict[fname] = {}
                                   if res_tup not in final_result_dict[fname]:
                                      final_result_dict[fname][res_tup] = []
                                   final_result_dict[fname][res_tup].append(exp_id+'_'+str(expr_row))
                                   #print ' org: ', fname, res_tup, exp_id+'_'+str(expr_row)
             
                                   if 0:
                                       #print all_hashkey_cols_dict.keys()
                                       otherelms = all_hashkey_cols_dict[fname][res_tup[0]+'#'+res_tup[1]]
                                       for otherelm in otherelms: 
                                           tfname, thashkey, tcolid = otherelm
                                           if tfname not in final_result_dict:
                                              final_result_dict[tfname] = {}
                                           if (thashkey, tcolid) not in final_result_dict[tfname]:
                                              final_result_dict[tfname][(thashkey, tcolid)] = []
                                           final_result_dict[tfname][(thashkey, tcolid)].append(exp_id+'_'+str(expr_row))
                                   
        return final_result_dict, pre_process_data                          





    def handle_analytics_scope(self, expr_ar, pre_process_data, mandatory_elms, scope_file_dict, gpath, exp_id):
       
         
        print 'handle_analytics_scope'
        #print scope_file_dict
        #sys.exit()
        final_result_dict = {} 
        all_parameters = map(lambda x:x['parameter'], expr_ar)
        expr_parameters = list(set(mandatory_elms[:] + all_parameters[:])) 
        expr_parameters = self.normalise_parameters(expr_parameters)
        for expr_row, expr in enumerate(expr_ar):
            #print '       -- expr: ', expr                
            scope = expr['scope'] 
            file_name_dict = {}
            if 'ROW' in scope: 
               fwd_ddict = {}  
               for taxoplvl, file_ddict in scope_file_dict[scope].items():   # filenames are grouped tree-level and like { 1 : { 'DATA': 1_RAW } }
                   data_dict = self.getdata_rowcol(gpath, file_ddict['DATA'], file_ddict['ROWCOL'],  'ROW', expr_parameters)
                   if expr['taxo'] == '*':
                      pass 
                   elif expr['taxo'] in data_dict:
                      kdata_dict = {}
                      kdata_dict[expr['taxo']] = data_dict[expr['taxo']]
                      data_dict = copy.deepcopy(kdata_dict)

                   fwd_ddict[taxoplvl] = copy.deepcopy(data_dict)
                   file_name_dict[taxoplvl] = file_ddict['DATA']

               for taxoplvl, data_dict in fwd_ddict.items():
                       new_data_dict, norm_parameter = self.handle_parameter(data_dict, expr['parameter'])  # new_data_dict : { plevel1: { param1: [] }, plevel2: { param1: [] } } -> param1 is the interested parameter
                       results_dict  = self.handle_filter_operator_condition(new_data_dict, expr, {}, norm_parameter, gpath)
                       for k, vsdict in results_dict.items():
                           for ph,  vals in vsdict.items():
                               for val in vals:
                                   res_key = []
                                   for mandatory_elm in [ 'hashkey', 'col']:
                                       res_key.append(val[mandatory_elm])
                                   res_tup = tuple(res_key) 
                                   if file_name_dict[taxoplvl] not in final_result_dict:
                                      final_result_dict[file_name_dict[taxoplvl]] = {}
                                   if res_tup not in final_result_dict[file_name_dict[taxoplvl]]:
                                      final_result_dict[file_name_dict[taxoplvl]][res_tup] = []
                                   final_result_dict[file_name_dict[taxoplvl]][res_tup].append(exp_id+'_'+str(expr_row))
               #print ' final_result_dict: ', final_result_dict
               #sys.exit()
        #sys.exit()
        return final_result_dict 


    def handle_normal_scope(self, expr_ar, pre_process_data, mandatory_elms, scope_file_dict, gpath, lookup_ddict, exp_id):

        print 'handle_normal_scope'
        #print scope_file_dict
        #sys.exit()
        final_result_dict = {} 
        all_parameters = map(lambda x:x['parameter'], expr_ar)
        #print expr_ar
        #sys.exit() 

        for expr in expr_ar:
            if 'DOCINFOPH' in expr['elmcondition']:
               all_parameters.append('docph')  

        expr_parameters = list(set(mandatory_elms[:] + all_parameters[:])) 
        expr_parameters = self.normalise_parameters(expr_parameters)
        for expr_row, expr in enumerate(expr_ar):
            print '       -- expr: ', expr                
            scope = expr['scope'] 
            file_name_dict = {}
            fwd_ddict = {}
            if 'COL' in scope:  
               if (scope, expr['parameter']) not in pre_process_data: 
                  pre_process_data[ (scope, expr['parameter'])  ] = {}
                  for taxoplvl, file_ddict in scope_file_dict[scope].items():   # filenames are grouped tree-level and like { 1 : { 'DATA': 1_RAW } }
                      print taxoplvl, file_ddict 
                      data_dict = self.getdata_rowcol(gpath, file_ddict['DATA'], file_ddict['ROWCOL'],  'COL', expr_parameters)
                      #pre_process_data[ (scope, expr['parameter'])  ][taxoplvl] = copy.deepcopy(data_dict)  
                      file_name_dict[taxoplvl] = file_ddict['DATA']
                      fwd_ddict[taxoplvl] = copy.deepcopy(data_dict)
               #fwd_ddict = pre_process_data[ (scope, expr['parameter']) ] # contains results from all the files from COL - with selected parameters only
            elif 'ROW' in scope: 
               print ' ROW- handle_normal_scope', expr  
               if (scope, expr['parameter']) not in pre_process_data: 
                  pre_process_data[ (scope, expr['parameter']) ] = {}
                  for taxoplvl, file_ddict in scope_file_dict[scope].items():   # filenames are grouped tree-level and like { 1 : { 'DATA': 1_RAW } }
                      data_dict = self.getdata_rowcol(gpath, file_ddict['DATA'], file_ddict['ROWCOL'],  'ROW', expr_parameters)
                      #pre_process_data[(scope, expr['parameter'])][taxoplvl] = copy.deepcopy(data_dict) 
                      file_name_dict[taxoplvl] = file_ddict['DATA']
                      fwd_ddict[taxoplvl] = copy.deepcopy(data_dict)
               #fwd_ddict = pre_process_data[(scope, expr['parameter']) ]  # contains results from all the files for ROW  - with selected parameters only
            else:
               print 'Not handled case - Error' 
               sys.exit()
            for taxoplvl, data_dict in fwd_ddict.items():
                   new_data_dict, norm_parameter = self.handle_parameter(data_dict, expr['parameter'])  # new_data_dict : { plevel1: { param1: [] }, plevel2: { param1: [] } } -> param1 is the interested parameter
                   results_dict  = self.handle_filter_operator_condition(new_data_dict, expr, lookup_ddict, norm_parameter, gpath)
                   for k, vsdict in results_dict.items():
                       for ph,  vals in vsdict.items():
                           for val in vals:
                               res_key = []
                               for mandatory_elm in mandatory_elms:
                                   res_key.append(val[mandatory_elm])
                               res_tup = tuple(res_key) 
                               if file_name_dict[taxoplvl] not in final_result_dict:
                                  final_result_dict[file_name_dict[taxoplvl]] = {}
                               if res_tup not in final_result_dict[file_name_dict[taxoplvl]]:
                                  final_result_dict[file_name_dict[taxoplvl]][res_tup] = []
                               final_result_dict[file_name_dict[taxoplvl]][res_tup].append(exp_id+'_'+str(expr_row))
                               #print file_name_dict[taxoplvl], ' == ', res_tup, ' -- ', exp_id+'_'+str(expr_row)
                               #sys.exit()
        return final_result_dict, pre_process_data                          



    def numerical_condition(self, dict1, dict2, expr_condition):
        try:
           f1 = float(dict1['clean_value']) 
           f2 = float(dict2['clean_value'])
        except:
           return 0
        #print 'in numerical_condition: ' 
        if expr_condition == "ValueGreaterThan":
           if f1 > f2:
              return 1 
           return 0

        elif expr_condition == "ValueLesserThan":
           if f1 < f2:
              return 1 
           return 0

        if expr_condition == "ABSValueGreaterThan":
           if abs(f1) > abs(f2):
              return 1 
           return 0

        elif expr_condition == "ABSValueLesserThan":
           if abs(f1) < abs(f2):
              return 1 
           return 0
        else:
           print 'TO DO numerical_condition'
           sys.exit() 
             

    def on_and_off(self, ar1, ar2):
        
           # ar1 - on
           # ar2 - off
           if 1:
                 dict1 = {}
                 dict2 = {}
                   
                 data_flg1 = 0
                 data_flg2 = 1
                 for elm in ar1:
                     if elm['gv_text'] != '':
                        data_flg1  = 1
                        dict1 = elm  
                        break
                 
                 for elm in ar2:
                     if elm['gv_text'] == '':
                        data_flg2  = 0
                        dict2 = elm
                        break
                 if (data_flg1 == 1) and (data_flg2 == 0):
                    return 1, dict1, dict2
           return 0 , {}, {}  
                 
                    

    def apply_numerical_expressions(self, sample_meta_expressions, scope_file_dict, gpath, hkeyfilemap):
        header_order = [ 'scope', 'parameter', 'filtervalue', 'gpoperator', 'elmcondition', 'description' ] #-> order of header for understanding purpose in the metaexpression
        mandatory_elms = [ 'hashkey', 'col', 'clean_value', 'gv_text']
        pre_process_data = {}
        final_result_dict = {}
        for exp_id, expr_ar in sample_meta_expressions.items():
            all_scopes = list(set(map(lambda x:x['scope'], expr_ar)))

            if len(all_scopes) != 1:
               print ' Something wrong with expression ', expr_ar 
               sys.exit()                  
            if 'REP' in all_scopes[0]:
                hkey_ddict = hkeyfilemap.get('REP', {})

            for expr_row, expr in enumerate(expr_ar): 
                taxo_id1 = expr['taxo1'] 
                taxo_id2 = expr['taxo2']

                if taxo_id1 not in hkey_ddict: continue
                if taxo_id2 not in hkey_ddict: continue
                     
                filedict1 = hkey_ddict[taxo_id1]
                filedict2 = hkey_ddict[taxo_id2]

                parameters = self.normalise_parameters( [ expr['parameter'] ] )
                parameter = parameters[0]
                taxo_id1_dict = self.getdata_taxohkey(gpath, filedict1['DATA'],  "hashkey", taxo_id1, parameter,  mandatory_elms[:]+[parameter])
                taxo_id2_dict = self.getdata_taxohkey(gpath, filedict2['DATA'],  "hashkey", taxo_id2, parameter,  mandatory_elms[:]+[parameter])

                all_phs = taxo_id1_dict.keys() + taxo_id2_dict.keys()
                all_phs = list(set(all_phs))
                for all_ph in all_phs:
                    ar1 = taxo_id1_dict.get(all_ph, [])
                    ar2 = taxo_id2_dict.get(all_ph, [])
                    #print all_phs, expr  
                    if expr['elmcondition'] in ['ON OFF', 'OFF ON']:
                       if not ar1: continue
                       if not ar2: continue
                       if 'ON OFF':
                          flg, dict1, dict2  = self.on_and_off(ar1, ar2)
                       elif 'OFF ON':
                          flg, dict2, dict1 = self.on_and_off(ar2, ar1)
                         
                       if (flg == 0): continue 

                       filename1 = filedict1['DATA'] 
                       hkey1 = (dict1['hashkey'], dict1['col'])
                       
                       if filename1 not in final_result_dict:
                          final_result_dict[filename1] = {}
                        
                       if hkey1 not in final_result_dict[filename1]:
                          final_result_dict[filename1][hkey1] = []

                       final_result_dict[filename1][hkey1].append(exp_id+'_'+str(expr_row))

                       filename2 = filedict2['DATA'] 
                       hkey2 = (dict2['hashkey'], dict2['col'])
                     
                       if filename2 not in final_result_dict:
                          final_result_dict[filename2] = {}
                        
                       if hkey2 not in final_result_dict[filename2]:
                          final_result_dict[filename2][hkey2] = []
                       final_result_dict[filename2][hkey2].append(exp_id+'_'+str(expr_row))

                    else:
                        # GreaterThan, LessThan 
                        if not ar1: continue
                        if not ar2: continue
                        for dict1 in ar1:
                            for dict2 in ar2:
                                flg = self.numerical_condition(dict1, dict2, expr['elmcondition'])
                                if flg:
                                   filename1 = filedict1['DATA'] 
                                   hkey1 = (dict1['hashkey'], dict1['col'])
                                   
                                   if filename1 not in final_result_dict:
                                      final_result_dict[filename1] = {}
                                    
                                   if hkey1 not in final_result_dict[filename1]:
                                      final_result_dict[filename1][hkey1] = []

                                   final_result_dict[filename1][hkey1].append(exp_id+'_'+str(expr_row))

                                   filename2 = filedict2['DATA'] 
                                   hkey2 = (dict2['hashkey'], dict2['col'])
                                 
                                   if filename2 not in final_result_dict:
                                      final_result_dict[filename2] = {}
                                    
                                   if hkey2 not in final_result_dict[filename2]:
                                      final_result_dict[filename2][hkey2] = []
                                   final_result_dict[filename2][hkey2].append(exp_id+'_'+str(expr_row))
                  

        return final_result_dict      
  


    def apply_num_analytics(self, sample_meta_expressions, scope_file_dict, gpath):
        header_order = [ 'scope', 'parameter', 'filtervalue', 'gpoperator', 'elmcondition', 'description' ] #-> order of header for understanding purpose in the metaexpression
        mandatory_elms = [ 'hashkey', 'col', 'row', 'clean_value', 'gv_text']
        pre_process_data = {}
        all_final_result_dict = {}
        for exp_id, expr_ar in sample_meta_expressions.items():
            all_scopes = list(set(map(lambda x:x['scope'], expr_ar)))

            if len(all_scopes) != 1:
               print ' Something wrong with expression ', expr_ar 
               sys.exit()                  

            if 0:#'FORMULA' in all_scopes[0]:
               #print all_scopes
               #print expr_ar  
               final_result_dict, pre_process_data = self.handle_formula_analytics_scope(expr_ar, pre_process_data, mandatory_elms, scope_file_dict, gpath, exp_id)
            else:
               final_result_dict = self.handle_analytics_scope(expr_ar, pre_process_data, mandatory_elms, scope_file_dict, gpath, exp_id)


            for k, vsdict in final_result_dict.items():
                if k not in all_final_result_dict:
                   all_final_result_dict[k] = {}
                for v, vs in vsdict.items():
                    if v not in all_final_result_dict[k]:
                       all_final_result_dict[k][v] = vs
                    else:
                       all_final_result_dict[k][v] += vs

        return all_final_result_dict      


    
    def apply_one_rule_data(self, all_oprres_data, expr_ar):
       
        result_dict = {} 
        for expr_row, expr_rule in enumerate(expr_ar):
            #print ' ======================================== '
            #print ' expr_row: ', expr_row
            #print expr_rule
            opr_fids = [] 
            if expr_rule['Ogpoperator'] == 'ON-OFF':
               for res_opr_data in all_oprres_data:
                   fid = res_opr_data[0]
                   if expr_rule['Ocondition'] == 'ON':
                      opr_data = res_opr_data[2]
                      allow_flg = 1
                      for opr in opr_data:
                          if opr['gv_text'] == '': # all are on
                             allow_flg = 0
                             break
                      if (allow_flg == 1):
                         opr_fids.append(fid)
                   if expr_rule['Ocondition'] == 'OFF':
                      opr_data = res_opr_data[2]
                      allow_flg = 0
                      for opr in opr_data:
                          if opr['gv_text'] == '': # one off
                             allow_flg = 1
                             break
                      if (allow_flg == 1):
                         opr_fids.append(fid)


            elif expr_rule['Ogpoperator'] == 'CellRes':
               for res_opr_data in all_oprres_data:
                   fid = res_opr_data[0]
                   opr_data = res_opr_data[2]
                   comp_dict = {} 
                   for i, opr in enumerate(opr_data):
                       comp_dict[i] = int(opr['res_flg'])
                   kres = self.compute_condition_count( comp_dict, expr_rule['Ocondition'])
                   if len(kres) == len(opr_data):
                      opr_fids.append(fid)
                          

            elif expr_rule['Ogpoperator'] == 'CellRep':
               for res_opr_data in all_oprres_data:
                   fid = res_opr_data[0]
                   opr_data = res_opr_data[2]
                   comp_dict = {} 
                   for i, opr in enumerate(opr_data):
                       comp_dict[i] = int(opr['rep_flg'])
                   kres = self.compute_condition_count( comp_dict, expr_rule['Ocondition'])
                   if len(kres) == len(opr_data):
                      opr_fids.append(fid)


            else:
               print 'Un handled Ogpoperator', [ expr_rule['Ogpoperator'] ]
               sys.exit()    


            res_fids = [] 
            if expr_rule['Rgpoperator'] == 'ON-OFF':
               for res_opr_data in all_oprres_data:
                   fid = res_opr_data[0]
                   if expr_rule['Rcondition'] == 'ON':
                      opr_data = res_opr_data[1] # res_data
                      allow_flg = 1
                      for opr in opr_data:
                          if opr['gv_text'] == '': # all are on
                             allow_flg = 0
                             break
                      if (allow_flg == 1):
                         res_fids.append(fid)
                   if expr_rule['Ocondition'] == 'OFF':
                      opr_data = res_opr_data[1] # res data
                      allow_flg = 0
                      for opr in opr_data:
                          if opr['gv_text'] == '': # one off
                             allow_flg = 1
                             break
                      if (allow_flg == 1):
                         res_fids.append(fid)
            elif expr_rule['Rgpoperator'] == 'checksum':
               chksum_dict = {} 
               for res_opr_data in all_oprres_data:
                   fid = res_opr_data[0]
                   for opr_data in res_opr_data[1]:  
                      opr_data = res_opr_data[1][0] # there is always one resultant in a formula
                      chksum = opr_data['chksum']
                      chksum_dict[fid] = float(chksum)
               kres = self.compute_condition_count(chksum_dict, expr_rule['Rcondition'])
               res_fids = kres

            elif expr_rule['Rgpoperator'] == 'CellRes':
               for res_opr_data in all_oprres_data:
                   fid = res_opr_data[0]
                   opr_data = res_opr_data[1] # resultant
                   comp_dict = {} 
                   for i, opr in enumerate(opr_data):
                       comp_dict[i] = int(opr['res_flg'])
                   kres = self.compute_condition_count( comp_dict, expr_rule['Rcondition'])
                   if len(kres) == len(opr_data):
                      res_fids.append(fid)
                          
            elif expr_rule['Rgpoperator'] == 'CellRep':
               for res_opr_data in all_oprres_data:
                   fid = res_opr_data[0]
                   opr_data = res_opr_data[1] # resultant
                   comp_dict = {} 
                   for i, opr in enumerate(opr_data):
                       comp_dict[i] = int(opr['rep_flg'])
                   kres = self.compute_condition_count( comp_dict, expr_rule['Rcondition'])
                   if len(kres) == len(opr_data):
                      res_fids.append(fid)

            else:
               print 'Un handled Ogpoperator'
               sys.exit()    

            inter_fids = list(set(res_fids).intersection(set(opr_fids)))
            result_dict[expr_row] = inter_fids 
        return result_dict 



    def apply_oprres_rules(self, sample_meta_expressions, scope_file_dict, gpath):
     
        final_result_dict = {} 
        for exp_id, expr_ar in sample_meta_expressions.items():
            all_scopes = list(set(map(lambda x:x['scope'], expr_ar)))
            if len(all_scopes) != 1:
               print ' Something wrong with expression ', expr_ar 
               sys.exit()                  

            if   all_scopes[0] == 'REP':
                 formula_filename = scope_file_dict['FORMULA-REP'] 
            elif all_scopes[0] == 'RAW': 
                 formula_filename = scope_file_dict['FORMULA-RAW'] 
            elif all_scopes[0] == 'RES':
                 formula_filename = scope_file_dict['FORMULA-RES'] 
           
         
            for fname, ddict in formula_filename.items():
                all_oprres_data = self.getformuladata(gpath, ddict['DATA'])
                row_results = self.apply_one_rule_data(all_oprres_data, expr_ar)
                #print fname
                #print row_results
                #sys.exit() 
                
                for row, res in row_results.items():
                    expr_id = exp_id+'_'+str(row)   
                    #for r in res:
                    sel_data = filter(lambda x:x[0] in res, all_oprres_data[:])
                    #print len(sel_data)

                    for (fid, res, opr_elms) in sel_data:
                        for e in res:
                            h, k = e['hashkeycol'].split('#')  
                            filename = e['fname']
                            if filename not in final_result_dict:
                               final_result_dict[filename] = {} 
                            if (h, k) not in final_result_dict[filename]:
                               final_result_dict[filename][(h,k)] = []
                            final_result_dict[filename ][(h,k)].append(expr_id)

                        
                 
        return final_result_dict   
            

    def handle_filter_number_label(self, num_results_dict, expr):

        if expr['filtervalue'] == '*':
           return num_results_dict
        else:
           print "TO DO not handled case - handle_filter_number_label"
           sys.exit() 

    def handle_hh_ngmwords(self, G1, G2,  elements, matchtype="EQUAL"):
        results = []    
        mydict = {}
        for element in elements:
            #[('5f7a164aae895017112876a482a77d2c#10', '4_41_1@R_10', (24, 10), '44_RAW.graphml'), '4_41_1', '4_41_1^x103_41', ('6949ae3dd8a0b3193bd223bb03d370de#9', '4_31_1@R', (15, 9), '64_RAW.graphml'), '4_31_1', '4_31_1^x109_31']
            from_element = element[2]
            to_element   = element[5] 
            if (from_element, to_element) not in mydict:
               mydict[(from_element, to_element)] = []  
            mydict[(from_element, to_element)].append(element)

        for from_to_element, sub_elements in mydict.items():
            ar1 = G1.vs.select(lambda v:v["node"]==from_to_element[0])
            ar2 = G2.vs.select(lambda v:v["node"]==from_to_element[1])
            for elm1 in ar1:
                if (elm1['VH'] != 'H'):   continue
                if (elm1['matchtype'] != 'MATCH_WORDS'): continue
                for elm2 in ar2:
                    if (elm2['VH'] != 'H'):   continue
                    if (elm2['matchtype'] != 'MATCH_WORDS'): continue
                    if (elm1['txt'] == elm2['txt']) and ( elm1['tx_level'] == elm2['tx_level']): 
                       for sub_elem in sub_elements:
                           results.append((sub_elem, elm1['tx_level'])) 
        return results 
                       
                   
    def handle_hv_ngmwords(self, G1, G2, elements, matchtype="EQUAL"):

        results = []    
        mydict = {}
        for element in elements:
            #[('5f7a164aae895017112876a482a77d2c#10', '4_41_1@R_10', (24, 10), '44_RAW.graphml'), '4_41_1', '4_41_1^x103_41', ('6949ae3dd8a0b3193bd223bb03d370de#9', '4_31_1@R', (15, 9), '64_RAW.graphml'), '4_31_1', '4_31_1^x109_31']
            from_element = element[2]
            to_element   = element[5] 
            if (from_element, to_element) not in mydict:
               mydict[(from_element, to_element)] = []  
            mydict[(from_element, to_element)].append(element)

        for from_to_element, sub_elements in mydict.items():
            ar1 = G1.vs.select(lambda v:v["node"]==from_to_element[0])
            ar2 = G2.vs.select(lambda v:v["node"]==from_to_element[1])
            for elm1 in ar1:
                if (elm1['VH'] != 'H'):   continue
                if (elm1['matchtype'] != 'MATCH_WORDS'): continue
                for elm2 in ar2:
                    if (elm2['VH'] != 'V'):   continue
                    if (elm2['matchtype'] != 'MATCH_WORDS'): continue
                    if (elm1['txt'] == elm2['txt']) and ( elm1['tx_level'] == elm2['tx_level']): 
                       for sub_elem in sub_elements:
                           results.append((sub_elem, elm1['tx_level'])) 
        return results 


 
    def handle_vv_ngrams(self, G1, G2, elements, matchtype="EQUAL"):

        results = []    
        mydict = {}
        for element in elements:
            #[('5f7a164aae895017112876a482a77d2c#10', '4_41_1@R_10', (24, 10), '44_RAW.graphml'), '4_41_1', '4_41_1^x103_41', ('6949ae3dd8a0b3193bd223bb03d370de#9', '4_31_1@R', (15, 9), '64_RAW.graphml'), '4_31_1', '4_31_1^x109_31']
            from_element = element[2]
            to_element   = element[5] 
            if (from_element, to_element) not in mydict:
               mydict[(from_element, to_element)] = []  
            mydict[(from_element, to_element)].append(element)

        for from_to_element, sub_elements in mydict.items():
            ar1 = G1.vs.select(lambda v:v["node"]==from_to_element[0])
            ar2 = G2.vs.select(lambda v:v["node"]==from_to_element[1])
            for elm1 in ar1:
                if (elm1['VH'] != 'V'):   continue
                if (elm1['matchtype'] != 'NGRAM'): continue
                for elm2 in ar2:
                    if (elm2['VH'] != 'V'):   continue
                    if (elm2['matchtype'] != 'NGRAM'): continue
                    if (elm1['txt'] == elm2['txt']) and ( elm1['tx_level'] == elm2['tx_level']): 
                       for sub_elem in sub_elements:
                           results.append((sub_elem, elm1['tx_level'])) 
        return results 


       

    def handle_hv_ngrams(self, G1, G2, elements, matchtype="EQUAL"):

        results = []    
        mydict = {}
        for element in elements:
            #[('5f7a164aae895017112876a482a77d2c#10', '4_41_1@R_10', (24, 10), '44_RAW.graphml'), '4_41_1', '4_41_1^x103_41', ('6949ae3dd8a0b3193bd223bb03d370de#9', '4_31_1@R', (15, 9), '64_RAW.graphml'), '4_31_1', '4_31_1^x109_31']
            from_element = element[2]
            to_element   = element[5] 
            if (from_element, to_element) not in mydict:
               mydict[(from_element, to_element)] = []  
            mydict[(from_element, to_element)].append(element)

        for from_to_element, sub_elements in mydict.items():
            ar1 = G1.vs.select(lambda v:v["node"]==from_to_element[0])
            ar2 = G2.vs.select(lambda v:v["node"]==from_to_element[1])
            for elm1 in ar1:
                if (elm1['VH'] != 'H'):   continue
                if (elm1['matchtype'] != 'NGRAM'): continue
                for elm2 in ar2:
                    if (elm2['VH'] != 'V'):   continue
                    if (elm2['matchtype'] != 'NGRAM'): continue
                    if (elm1['txt'] == elm2['txt']) and ( elm1['tx_level'] == elm2['tx_level']): 
                       for sub_elem in sub_elements:
                           results.append((sub_elem, elm1['tx_level'])) 
        return results 


 

    def handle_gpoperator_number_label(self, num_results_dict, expr, gpath_label):
        res_keys = num_results_dict.keys() 
        #res_keys = [ ('4_12_1', '4_54_1') ]
        match_results = {} 
        for res_key in res_keys:
            #print 'pair_key: ', res_key
            match_results[res_key] = [] 
            mydict = {}      
            for result in num_results_dict[res_key][:]:
                #[('5f7a164aae895017112876a482a77d2c#10', '4_41_1@R_10', (24, 10), '44_RAW.graphml'), '4_41_1', '4_41_1^x103_41', ('6949ae3dd8a0b3193bd223bb03d370de#9', '4_31_1@R', (15, 9), '64_RAW.graphml'), '4_31_1', '4_31_1^x109_31']
                #ar.append(result)
                fname1 = result[1]
                fname2 = result[4]
                if (fname1, fname2) not in mydict:
                   mydict[(fname1, fname2)] = []     
                mydict[(fname1, fname2)].append(result)
            
            prev_hh_dict = {}      
            for fnames, ar in mydict.items():
                #print 'Graph1: ', gpath_label+res_key[0]+'.graphml'
                #print 'Graph2: ', gpath_label+res_key[1]+'.graphml'

                G1 = Graph.Read_GraphML(gpath_label+fnames[0]+'.graphml')
                G2 = Graph.Read_GraphML(gpath_label+fnames[1]+'.graphml')
                
                #print gpath_label+fnames[0]+'.graphml'
                #print gpath_label+fnames[1]+'.graphml'   

                if expr['gpoperator'] == 'H:MWORDS_H:MWORDS': 
                   results = self.handle_hh_ngmwords(G1, G2, ar, expr['elmcondition'])
                   match_results[res_key] = results[:]
                elif expr['gpoperator'] == 'H:MWORDS_V:MWORDS':
                   results = self.handle_hv_ngmwords(G1, G2, ar, expr['elmcondition'])
                   match_results[res_key] = results[:]
                elif expr['gpoperator'] == 'V:NGRAM_V:NGRAM':
                   results = self.handle_vv_ngrams(G1, G2, ar, expr['elmcondition'])
                   match_results[res_key] = results[:]
                elif expr['gpoperator'] == 'H:NGRAM_V:NGRAM':
                   results = self.handle_hv_ngrams(G1, G2, ar, expr['elmcondition'])
                   match_results[res_key] = results[:]
                else:
                   print "to do handle case handle_gpoperator_number_label: ", expr['gpoperator']
                   sys.exit()
                del G1
                del G2
                #sys.exit()
        return match_results 


    def apply_label_number_expression(self, expressions, scope_file_dict, gpath_label, gpath_labeltonum, gpath, companyid, doc_id):

        os.system('rm -rf '+gpath_labeltonum)
        os.system('mkdir -p '+gpath_labeltonum)
          

        all_results = []
        #print 'apply_label_number_expression'
        for exp_id, expr_ar in expressions.items():
            #print
            #print
            #print '*****************************************************'  
            #print ' exp_id: ', exp_id   
            for exp_ind , expr in enumerate(expr_ar):
                # parameter is taxonomy assumed
                print '     ============================================ '
                print '     dexpression: ', exp_ind
                print '     dexpression expr: ', expr
                #print gpath_label 
                #if expr['parameter'] == 'MATCH_WORDS':
                num_gp_dict = {} 
                if 1: 
                   level_fdict = scope_file_dict['COL-'+expr['scope']] 
                   table_ids = []
                      
                   prop_ar_dict = {}
                   map_values = {}     
                   for taxo, fdict in level_fdict.items():
                        data_fname = fdict['DATA']
                        fpath = gpath+data_fname 
                        #print '   gggfpath: ', fpath
                        G = Graph.Read_GraphML(fpath)
                        prop_ar_dict[data_fname]= {}
                        for v in G.vs:
                            table_id = v['table_id']
                            table_ids.append(table_id)   
                            #print v['clean_value'], table_id+'^'+v['gv_xml_ids']
                            if v['clean_value']  != '': 
                               map_values[table_id+'^'+v['gv_xml_ids']] = [ float(v['clean_value']), v['gv_text'], data_fname , int(v['gv_xml_ids'].split('_')[-1])]
                               if (table_id+'^'+v['gv_xml_ids'] not in prop_ar_dict[data_fname]):
                                  prop_ar_dict[data_fname][table_id+'^'+v['gv_xml_ids']] = []     
                               prop_ar_dict[data_fname][table_id+'^'+v['gv_xml_ids']].append((v['line_hashkey'], int(v['col']), v['abs_hashkey'], v['hashkeycol'], v['group_ids']))
                              

                   table_ids = list(set(table_ids))
                   for table_id in table_ids:
                       mpath = gpath_label + table_id+'.graphml'
                       try:
                           G = Graph.Read_GraphML(mpath)
                       except:
                           continue
                       for v in G.vs:
                           txl = (v['tx_level'], v['VH'])
                           node = v['node']    
                           if txl not in num_gp_dict:
                              num_gp_dict[txl] = []  
                           num_gp_dict[txl].append(node)
       
                if  expr['elmcondition'] == 'G:DIST':
                    # within taxonomy and G-Computation   
                    for txl, nodes in num_gp_dict.items():
                        #if txl[0] != '88.2': continue
                        #print ' ===================================== '
                        #print ' txl: ', txl
                          
                        vals = []    
                        for node in nodes:
                            if node not in map_values: continue
                            #print '    node: ', node, ' === ', map_values[node]
                            pg = [ map_values[node][3], map_values[node][0], map_values[node][1], map_values[node][2], txl , node]
                            vals.append(pg)
                        vals.sort()
                        results = self.simple_distribution_smalltobig(vals, exp_id+'_'+str(exp_ind))
                        all_results += results[:]
       


        display_flg = 1
        if display_flg:   
           html_display_path = '/var/www/html/avinash/label_number_comp/'+companyid+'/'+doc_id+'/'
           os.system('rm -rf '+html_display_path)
           os.system('mkdir -p '+html_display_path)
             

        display_dict = {}
        formula_cnt = 0
        print 'FINAL RESULT'              
        form_ddict = {} 
        for result in all_results:
            formula_cnt = formula_cnt + 1  
            ds_header = [ "ftype", "taxo_level", "line_key", "res_flag" , "filenames", "phformtype", "formexpr", "fid", "colid", "rid", "abs_key", "type", "ID", "hashkeycol", "tablexmlid" , "group_ids"]
            res_fname = result[0][3]
            res_txmlid = result[0][5]
            res_ddict_ar, abs_key = self.form_datastructure_comp(res_fname, res_txmlid, prop_ar_dict, ds_header, 1)
            form_ddict_ar = res_ddict_ar[:]
            fid = abs_key 
            for opr_result in result[1][:]:
                opr_fname = opr_result[3]
                opr_txmlid = opr_result[5]
                opr_ddict_ar, abs_key2 = self.form_datastructure_comp(opr_fname, opr_txmlid, prop_ar_dict, ds_header, 0)
                form_ddict_ar += opr_ddict_ar[:]

            node_dict = { "ftype":result[2], "taxo_level":"", "line_key":"", "res_flag":"" , "filenames":"", "phformtype":"", "formexpr":result[3], "fid":"", "colid":"", "rid":fid+"-"+str(formula_cnt), "abs_key":"", "type":"FORMULA", "ID":formula_cnt, "hashkeycol":"", "tablexmlid":"" , "group_ids":"" }
             
            for ddict in form_ddict_ar:
                ddict["rid"] = fid+"-"+str(formula_cnt) 

            form_ddict_ar.append(node_dict)
            if res_fname not in form_ddict:
               form_ddict[res_fname] = []
            form_ddict[res_fname] += form_ddict_ar[:]

            if display_flg:
               if result[3]:
                  if result[3] not in display_dict:
                     display_dict[result[3]] = [] 
                  display_dict[result[3]].append(result)
         
        if 1:
            import populate_table_lets_DB
            tmpobj  = populate_table_lets_DB.Populate()
            tmpobj.populate_rawdb(companyid, doc_id, display_dict)
                    
        if display_flg:
           for dk, vs in display_dict.items():
               print html_display_path+dk+'.html'
               f = open(html_display_path+dk+'.html', 'w')
               f.write('<html>')                          

               for v in vs:
                   header = [ 'Number', 'Taxonomy', 'TableXMLid' ]
                    
                   #print ' res: ', v[0]
                   #print ' opr: ', v[1]
                   f.write('<br><hr><br><table border=1>')
                   f.write('<tr>')
                   for h in header:
                       f.write('<td bgcolor=yellow><b>'+h+'</b></td>')
                   f.write('</tr>')
                         
                   f.write('<tr bgcolor=cyan>')
                   f.write('<td>'+v[0][2]+'</td>')
                   f.write('<td>'+str(v[0][4])+'</td>')
                   f.write('<td>'+v[0][5]+'</td>')
                   f.write('</tr>')

                   for e in v[1]:
                       f.write('<tr>')
                       f.write('<td>'+e[2]+'</td>')
                       f.write('<td>'+str(e[4])+'</td>')
                       f.write('<td>'+e[5]+'</td>')
                       f.write('</tr>')
                   f.write('</table>') 
                      
               f.write('</html>')                          
               f.close()

        for res_fname, form_ddict_ar in form_ddict.items():
            all_keys = form_ddict_ar[0].keys() 
            prop_val_dict = {}
            for all_key in all_keys:
                prop_val_dict[all_key] = map(lambda x:x[all_key], form_ddict_ar[:])

            G = Graph()
            G.add_vertices( len(form_ddict_ar) )
            G.to_directed(False)
            G.to_undirected(True)
            for k, vs in prop_val_dict.items():  
                G.vs[k] = vs
            G.write_graphml(gpath_labeltonum+res_fname)
            print 'Gpath: ', gpath_labeltonum+res_fname
            del G    
 
        return all_results  


    def form_datastructure_comp(self, res_fname, res_txmlid, prop_ar_dict, ds_header, res_flag=0):
        prop_vals  = prop_ar_dict[res_fname][res_txmlid]  
        ddict_ar = [] 
        for ar in prop_vals:
            line_hashkey, col, abs_hashkey, hashkeycol, group_ids = ar[:]
            ddict = {}
            for ds_h in ds_header:
                if ds_h ==  "tablexmlid":   
                   ddict[ds_h] = res_txmlid 
                elif ds_h ==  "hashkeycol":   
                   ddict[ds_h] = hashkeycol
                elif ds_h ==  "abs_key":   
                   ddict[ds_h] = abs_hashkey 
                elif ds_h ==  "colid":   
                   ddict[ds_h] = col
                elif ds_h == "filenames":
                   ddict[ds_h] = res_fname 
                elif ds_h == "res_flag":
                   ddict[ds_h] = res_flag     
                elif ds_h == "line_key":
                   ddict[ds_h] = line_hashkey 
                elif ds_h == "group_ids":
                   ddict[ds_h] = group_ids  
                elif ds_h == "type":
                   ddict[ds_h] = "NODE"
                else:
                   ddict[ds_h] = ""
            ddict_ar.append(copy.deepcopy(ddict))
        return ddict_ar, abs_hashkey                
            
           

         

    def simple_distribution_smalltobig(self, vals, expr_id):
        results = []
        for i1, cur_val in enumerate(vals):
            if i1 > 2:
               # cur_val must be bigger than all the numbers above this - abswise
               sub_vals = vals[:i1]
               map_sub_vals = map(lambda x:abs(x[1]), sub_vals[:])

               map_sub_vals.reverse()
               sub_vals.reverse()
               shortlisted_sub_vals = [] 
               for j, val in enumerate(map_sub_vals):
                   if val >= abs(cur_val[1]):  break
                   shortlisted_sub_vals.append(sub_vals[j])
 
               if shortlisted_sub_vals: 
                     
                   temp_vals = map(lambda x:x[1], shortlisted_sub_vals[:])
                   if (abs(cur_val[1] - sum(temp_vals)) < 0.01):
                      #print '=============================================='
                      #print ' Result: ', shortlisted_sub_vals
                      #print ' sum value: ', cur_val[1]
                      #print ' only_vals: ', temp_vals         
                      results.append((cur_val, shortlisted_sub_vals, 'DIST', expr_id))   
        return results 
                 
                
           
             

    def apply_number_label_dexpressions(self, expressions, scope_file_dict, gpath_numtolabel, gpath):
        # we know that scope == cell always - else it will be triggered as error

        gp_id = 0
        storage_ds = {}  
        pre_store = {}
        for exp_id, expr_ar in expressions.items():
            #if exp_id != 'NumLabEq2': continue 
            for exp_ind , expr in enumerate(expr_ar):
                print ' ============================================ '
                print 'dexpression: ', exp_ind
                print 'dexpression expr: ', expr
                #sys.exit()
                mykey = expr['classname']+'_'+expr['expressionid']+'_'+expr['index'] 
                #print scope_file_dict.keys()
                for ttype in ['RAW']: # later if required add 'REP', 'RES'
                    level_fdict = scope_file_dict['COL-'+ttype] 
                    table_id_ddict = {}
                    for taxo, fdict in level_fdict.items():
                        data_fname = fdict['DATA']
                        fpath = gpath_numtolabel+data_fname 
                        print '   gggfpath: ', fpath
                        G = Graph.Read_GraphML(fpath)
                        table_dict = {}   
                        for pos_val in G.vs:
                            #print pos_val 
                            #print mykey  
                            if pos_val[mykey].strip(): 
                               row_col1 = (int(pos_val['row']), int(pos_val['col']))
                               group_id1 = pos_val['group_ids']
                               row1 = row_col1[0]
                               col1 = row_col1[1]

                               hashcol1 = pos_val['hashkey']+'#'+str(col1)

                               for elm in pos_val[mykey].split('$$'):
                                   elm_sp = elm.split('@')
                                   #print elm_sp
                                   hashcol2, fname2, txnlvl, npairid = elm_sp
                                   table_id1 = npairid.split('*')[0].split('^')[0]
                                   #if table_id1 != '4_37_4': continue    # comment later
                                   table_id2 = npairid.split('*')[1].split('^')[0]

                                   row_col2 = eval(npairid.split('*')[3])
                                   row2 = row_col2[0]
                                   col2 = row_col2[1]
                                   group_id2 = npairid.split('*')[5]
                                   group_id2 = group_id2.replace('A', '@')   
                                   #print row_col1 
                                   #print row_col2 
                                   #print npairid
                                   #print group_id1, ' == ', group_id2 
                                   #sys.exit()       
                                   #print ' table_id1: ', table_id1
                                   #print ' table_id2: ', table_id2
                                   #print ' hashcol2: ', hashcol2  
                                   #fname2path = gpath + fname2 
                                   #print fname2path  
                                   #G1 = Graph.Read_GraphML(fname2path)
                                   #shortlisted_vs1 = G1.vs.select(lambda v:v["hashkeycol"]==hashcol2)
                                   #print shortlisted_vs1   
                                   #for short_v in shortlisted_vs1:
                                   if  1:
                                       #print short_v 
                                       #row_col2 = (int(short_v['row']), int(short_v['col']))
                                       #row2 = row_col2[0]
                                       #col2 = row_col2[1]
                                       #group_id2 = short_v['group_ids']
                                       if expr['APPLY_SCOPE'] == 'COL_COL':
                                          myeq = (table_id1, table_id2, txnlvl, group_id1, group_id2, row1, row2)
                                          if myeq not in table_dict:
                                              table_dict[myeq] = {} 
                                          if (col1, col2) not in table_dict[myeq]:
                                              table_dict[myeq][(col1, col2)] = [] 
                                          table_dict[myeq][(col1, col2)].append(elm+'@COL_COL@'+hashcol1)
                                       elif expr['APPLY_SCOPE'] == 'ROW_COL':
                                          #myeq = (table_id1, table_id2, txnlvl, col1, row2)
                                          myeq = (table_id1, table_id2, txnlvl, group_id1, group_id2, col1, row2)
                                          if myeq not in table_dict:
                                              table_dict[myeq] = {} 
                                          if (row1, col2) not in table_dict[myeq]:
                                              table_dict[myeq][(row1, col2)] = []
                                          table_dict[myeq][(row1, col2)].append(elm+'@ROW_COL@'+hashcol1)
                                       elif expr['APPLY_SCOPE'] == 'ROW_ROW':
                                          #myeq = (table_id1, table_id2, txnlvl, col1, col2)
                                          myeq = (table_id1, table_id2, txnlvl, group_id1, group_id2, col1, col2)
                                          if myeq not in table_dict:
                                              table_dict[myeq] = {} 
                                          if (row1, row2) not in table_dict[myeq]:
                                              table_dict[myeq][(row1, row2)] = []
                                          table_dict[myeq][(row1, row2)].append(elm+'@ROW_ROW@'+hashcol1)
                                       elif expr['APPLY_SCOPE'] == 'COL_ROW':
                                          #myeq = (table_id1, table_id2, txnlvl, row1, col2)
                                          myeq = (table_id1, table_id2, txnlvl, group_id1, group_id2, row1, col2)
                                          if myeq not in table_dict:
                                              table_dict[myeq] = {} 
                                          if (col1, row2) not in table_dict[myeq]:
                                              table_dict[myeq][(col1, row2)] = [] 
                                          table_dict[myeq][(col1, row2)].append(elm+'@COL_ROW@'+hashcol1)
                                   #del G1 
                        del G   
                        if 1:
                            for tabpair, coldict in table_dict.items():
                                print '============TABLE PAIR=============='
                                print ' xxxfpath: ', fpath
                                print tabpair , coldict.keys() 
                                mydict = {} 
                                for rcpair, vs in coldict.items():
                                    #print '      rcpair: ', rcpair 
                                    if rcpair[0] not in mydict:
                                       mydict[rcpair[0]] = []  
                                    mydict[rcpair[0]].append(rcpair[1])

                                #print mydict    
                                keys = mydict.keys()
                                if len(keys) == 1: continue
                                ar = map(lambda x:mydict[x], keys[:])
                                len_ar = map(lambda x:len(x), ar[:])

                                if len(len_ar) == 1: continue
                                 
                                mul_len = 1
                                for len_elm in len_ar:
                                    mul_len = mul_len * len_elm
 
                                print len_ar  , ' mul_len: ', mul_len  
                                if mul_len > 20:
                                   continue
                                pos_ar = self.timer_restricted(len_ar)
                                #print 'pos_ar: ', pos_ar  
                                for pos_elm in pos_ar:
                                    pos_ar = []
                                    for i1, pelm in enumerate(pos_elm):
                                        pos_ar.append(mydict[keys[i1]][pelm])
                                    unq_pos_ar = list(sets.Set(pos_ar))
                                    if len(pos_ar) != len(unq_pos_ar): continue
                                    if pos_ar:
                                       gp_id += 1 
                                    print '======================================================================', tabpair  
                                    print '======================================================================',  'colkeys: ', coldict.keys() 
                                    print ' ++ ', pos_ar, ' -- ', pos_elm, ' group_id: ', gp_id 
                                    for i1, pos_elm in enumerate(pos_ar):
                                        mykey2 = (keys[i1], pos_elm)
                                        #print '        *** mykey: ', mykey  , coldict[mykey] 
                                        #print data_fname
                                        print ' ----> ', mykey2  
                                        for e in coldict[mykey2]:
                                            e_sp = e.split('@')
                                            print '            +++++++++++++ ', e_sp 
                                            to_key = tuple(e_sp[-1].split('#')) # fromkey
                                            store_elm = '@'.join(e_sp[:-1])+'@'+str(gp_id)
                                            if data_fname not in storage_ds:
                                               storage_ds[data_fname] = {}
                                            if to_key not in storage_ds[data_fname]:
                                               storage_ds[data_fname][to_key] = {}
                                            if exp_id+'_'+str(exp_ind) not in storage_ds[data_fname][to_key]:
                                               storage_ds[data_fname][to_key][exp_id+'_'+str(exp_ind)] = []
                                            storage_ds[data_fname][to_key][exp_id+'_'+str(exp_ind)].append(store_elm) 
                                            print '     storage: ', data_fname, to_key, exp_id+'_'+str(exp_ind), store_elm 
                                    #print storage_ds
        
        return storage_ds 


    def timer_restricted(self, len_ar):
          # timer restricted with len_ar
          for e in len_ar:
              if e <= 0:
                 return []
              if type(e) != type(1):
                 return []
          init_ar = []
          for i in range(0, len(len_ar)):
              init_ar.append(0)

          ar = [ init_ar[:] ]
          flg = 1
          while flg:
                flg = 0
                inc_ind = -1
                for i, e in enumerate(init_ar):
                    if (e + 1 < len_ar[i]):
                       inc_ind = i
                       break
                if (inc_ind != -1):
                   flg = 1
                   init_ar[inc_ind] += 1
                   for j in range(0, inc_ind):
                       init_ar[j] = 0
                   ar.append(init_ar[:])
          return ar


    def apply_number_label_expressions(self, expressions, scope_file_dict, gpath, gpath_neq, gpath_label):
        # we know that scope == cell always - else it will be triggered as error

        storage_ds = {}  
        pre_store = {}
        for exp_id, expr_ar in expressions.items():
            #if exp_id != 'NumLabEq2': continue # comment later 


            #print exp_id, expr_ar 
            #sys.exit()  
            all_scopes = list(set(map(lambda x:x['scope'], expr_ar)))
            if len(all_scopes) != 1:
               print ' Something wrong with expression in scope apply_number_label_expressions ', expr_ar 
               sys.exit()                  
            
            print all_scopes
            print exp_id
            print expr_ar
            #sys.exit()
            for exp_ind , expr in enumerate(expr_ar):
                mykey = 'COL-'+all_scopes[0]
                if 'FACTOR' in expr['parameter']:
                   pfactor = int(expr['parameter'].split('FACTOR:')[1])
                else:
                   print ' Something wrong with expression in parameter apply_number_label_expressions ', expr_ar 
                   sys.exit()                  
                
                if (all_scopes[0], pfactor) not in     pre_store:
                   #print 'before get_cells_numeq: ', ( gpath, gpath_neq, all_scopes[0], pfactor)  
                   num_results_dict = self.get_cells_numeq( gpath, gpath_neq, scope_file_dict, all_scopes[0], pfactor)
                   pre_store[(all_scopes[0], pfactor)] = num_results_dict  
                
                num_results_dict =  pre_store[(all_scopes[0], pfactor)] 
                num_results_dict = self.handle_filter_number_label(num_results_dict, expr) 
                match_results = self.handle_gpoperator_number_label(num_results_dict, expr, gpath_label)
                for res_key, store_results in match_results.items():
                    #print '   res_key: ', res_key 
                    for store_result in store_results:
                        #print '       --- store_result: ', store_result        

                        from_fname = store_result[0][0][3]
                        fromkey = tuple(store_result[0][0][0].split('#'))
                        to_fname   =  store_result[0][3][3]
                        to_hkeycol =  store_result[0][3][0]
                        txn_lvl = store_result[1] 
                        gpid1 = store_result[0][0][1]
                        gpid2 = store_result[0][3][1]
                        gpid1 = gpid1.replace('@', 'A')
                        gpid2 = gpid2.replace('@', 'A')
                          
                        npairid = store_result[0][2]+'*'+ store_result[0][5]+'*'+str(store_result[0][0][2])+'*'+str(store_result[0][3][2])+'*'+gpid1+'*'+gpid2
                        #print npairid     
                        #sys.exit()  
                        #print from_fname, fromkey
                        #print to_fname, to_hkeycol 
                        #sys.exit()
                        #from_hkeycol, from_fname, fromkey, from_element, to_hkeycol, to_fname, tokey, to_element = store_result[0] 
                        #txn_lvl = store_result[1] 

                        if from_fname not in storage_ds:
                           storage_ds[from_fname] = {}
                        if fromkey not in storage_ds[from_fname]:
                           storage_ds[from_fname][fromkey] = {}
                        if exp_id+'_'+str(exp_ind) not in storage_ds[from_fname][fromkey]:
                           storage_ds[from_fname][fromkey][exp_id+'_'+str(exp_ind)] = []
                        storage_ds[from_fname][fromkey][exp_id+'_'+str(exp_ind)].append(to_hkeycol+'@'+to_fname+'@'+txn_lvl+'@'+npairid)    
                        #print 'storing: ', fromkey, to_hkeycol+'@'+to_fname+'@'+txn_lvl+'@'+npairid 
        
        return storage_ds 

      
    def apply_meta_expressions(self, sample_meta_expressions, scope_file_dict, gpath, lookup_ddict):
        header_order = [ 'scope', 'parameter', 'filtervalue', 'gpoperator', 'elmcondition', 'description' ] #-> order of header for understanding purpose in the metaexpression
        mandatory_elms = [ 'hashkey', 'col', 'line_hashkey', 'abs_hashkey']
        mandatory_elms = [ 'hashkey', 'col']
        pre_process_data = {}
        all_final_result_dict = {}
        for exp_id, expr_ar in sample_meta_expressions.items():
            all_scopes = list(set(map(lambda x:x['scope'], expr_ar)))
            if len(all_scopes) != 1:
               print ' Something wrong with expression ', expr_ar 
               sys.exit()                  

            #print '==========================================================='
            #print 'Executing: ', exp_id, expr_ar 
            if 'FORMULA' in all_scopes[0]:  # convention changed not used anymore
               # all relationships related to formula need to be given in opr_res formulas
               print 'Discontinued - Avinash'
               sys.exit()
               #print all_scopes
               #print expr_ar  
               final_result_dict, pre_process_data = self.handle_formula_scope(expr_ar, pre_process_data, mandatory_elms, scope_file_dict, gpath, lookup_ddict, exp_id)
            else:
                final_result_dict, pre_process_data = self.handle_normal_scope(expr_ar, pre_process_data, mandatory_elms, scope_file_dict, gpath, lookup_ddict, exp_id)

            pre_process_data = {} 
            for k, vsdict in final_result_dict.items():
                if k not in all_final_result_dict:
                   all_final_result_dict[k] = {}
                for v, vs in vsdict.items():
                    if v not in  all_final_result_dict[k]: 
                       all_final_result_dict[k][v] = vs
                    else:  
                       all_final_result_dict[k][v] += vs
            #sys.exit()
        return all_final_result_dict      
 


    def update_result_graph_group(self, final_result_dict, sample_expr_type, gpath):
        mandatory_elms = [ 'hashkey', 'col', 'line_hashkey', 'abs_hashkey']
        results_path = gpath
        for filename, hdict in final_result_dict.items():
            #if filename != '15_RAW.graphml': continue 
            fpath = results_path+filename
            G = Graph.Read_GraphML(fpath)
            vals_ar = []
            all_hashkeys = []
            store_vals_ar = []  
            for res_tup, vals in hdict.items():
                hashkey = res_tup[0]
                col = res_tup[1]
                all_hashkeys.append(hashkey+'#'+str(int(col)))
                vals_ar.append(vals.keys())
                store_vals_ar.append(copy.deepcopy(vals))
            #print all_hashkeys        
            shortlisted_vs = G.vs.select(lambda v:v["hashcolkeys"] in all_hashkeys)
            index_ar =  [ v.index for v in shortlisted_vs]
            #print '======================================='  
            #print 'shortlisted: ', shortlisted_vs 
            #print 'index_ar: ', index_ar    
            for v in shortlisted_vs:
                i = all_hashkeys.index(v['hashkey']+'#'+str(int(v['col'])))
                #print all_hashkeys[i] 
                vals = vals_ar[i]
                mdict = store_vals_ar[i]
                #print v
                #print vals
                for val in vals: 
                    mystr = '$$'.join(mdict[val])                    
                    #print ' adding: ', mystr 

                    #for mv_elm in mdict[val][:]:
                    #    print '     mv_elm: ', mv_elm   

                    #sys.exit() 
                    G.vs[v.index][sample_expr_type+"_"+val] = mystr 
                    #print 'Adding: ', sample_expr_type+"_"+val, '90'
                    #print 'updated: ', G.vs[ind] 
            print 'Deleted: ', fpath   
            os.system('rm -rf '+fpath)
            print 'update_result_graph: ', fpath 
            G.write_graphml(fpath)
            print 'done update_result_graph: ', fpath 
            del G  


 
    def update_result_graph(self, final_result_dict, sample_expr_type, gpath):
        mandatory_elms = [ 'hashkey', 'col', 'line_hashkey', 'abs_hashkey']
        results_path = gpath+'lbvresults/'
        for filename, hdict in final_result_dict.items():
            fpath = results_path+filename
            G = Graph.Read_GraphML(fpath)
            vals_ar = []
            all_hashkeys = []
            for res_tup, vals in hdict.items():
                hashkey = res_tup[0]
                col = res_tup[1]
                all_hashkeys.append(hashkey+'#'+str(int(col)))
                vals_ar.append(vals[:])
            shortlisted_vs = G.vs.select(lambda v:v["hashcolkeys"] in all_hashkeys)
            index_ar =  [ v.index for v in shortlisted_vs]
            for i, ind in enumerate(index_ar):
                vals = vals_ar[i]
                for val in vals: 
                    G.vs[ind][sample_expr_type+"_"+val] = 1  
            print 'Deleted: ', fpath   
            os.system('rm -rf '+fpath)
            print 'update_result_graph: ', fpath 
            G.write_graphml(fpath)
            print 'done update_result_graph: ', fpath 
            del G  
         
    def update_result_validation(self, validation_expressions, gpath):
        prefix = 'Validation_'
        results_path = gpath+'lbvresults/'
        #print results_path  
        results_files = os.listdir(results_path)
        print 'in update_result_validation', results_files  
        for results_file in results_files:
            if '.graphml' not in results_file: continue
            #print ' results path: ', results_path+results_file
            G = Graph.Read_GraphML(results_path+results_file)
            allhashcolkeys = G.vs["hashcolkeys"]
            attributes =  G.vs.attributes()
            update_flag = 0
            for vname, vdicts in validation_expressions.items():
                no_result = 0 
                validation_info = {}
                iter_cnt = -1 
                init_results = []
                for vdict in vdicts:
                    #print vdict 
                    if 'validationflag'  in vdict:
                       validation_info = vdict['validationflag']
                       continue  
                    else: 
                       iter_cnt += 1 
                       classname = vdict['classname']
                       expressionid = vdict['expressionid']
                       status = vdict['status']
                       ind = vdict['index']
                       mykey = classname+"_"+expressionid+"_"+str(ind)
                       if mykey not in attributes: 
                          no_result = 1 
                          break 
                       myval = status  
                       shortlisted_vs = G.vs.select(lambda v:v[mykey]==1)
                       pass_colkeys = []
                       for shortlisted_v in shortlisted_vs:
                           pass_colkeys.append(shortlisted_v["hashcolkeys"])
                       if status.lower() != "true": 
                          pass_colkeys = list(set(allhashcolkeys) - set(pass_colkeys))
                       if iter_cnt == 0:
                          init_results = pass_colkeys
                       else:
                          #print init_results
                          #print pass_colkeys
                          s1 = set(init_results)
                          s2 = set(pass_colkeys)
                          init_results = list(s1.intersection(s2))
                       #print ' ++ ', mykey
                       #print G.vs[mykey]
                #print 'no_result: ', no_result    
                if (no_result == 1): 
                    continue  
                if  init_results:
                    shortlisted_vs = G.vs.select(lambda v:v["hashcolkeys"] in init_results)
                    index_ar =  [ v.index for v in shortlisted_vs]
                    for ind in index_ar:
                        G.vs[ind][prefix+vname] = validation_info
                        update_flag = 1  
           
            #print ' update_flag: ', update_flag  
            if  update_flag == 1:
                print 'Deleted: ', results_path+results_file 
                os.system('rm -rf '+results_path+results_file)
                print 'validation update_result_graph: ', results_path+results_file 
                G.write_graphml(results_path+results_file)
                print 'done validation - update_result_graph: ', results_path+results_file
            del G 



    def group_expression(self, validation_expressions):

            new_validation_expressions = {}  
            for k, vs in validation_expressions.items():
                ar = []
                ddict = {}
                for v in vs:
                    #print v
                    ddict2   = {}
                    for vk, vv in v.items():
                        if vk == 'validationflag':
                           ddict['validationflag'] = vv
                        elif vk == 'status':
                           ddict2['status'] = vv.lower()
                        else:
                           ddict2[vk] = vv
                    ar.append(copy.deepcopy(ddict2))
                ar.append(copy.deepcopy(ddict)) 
                new_validation_expressions[k] = ar[:]
            return new_validation_expressions 

             



    def get_expressions(self):
        if  1:
            sh = shelve.open('/home/avinash/lbv_test.sh')
            rule_data_dict = sh['data']
            sh.close()

            #print rule_data_dict.keys()
            #sys.exit() 

            number_label_expression = {'NumberLabelGroupRules': rule_data_dict['NumberLabelGroupRules']}
            number_label_dexpression = {'NumberLabelGroupDRules': rule_data_dict['NumberLabelGroupDRules']}


            label_number_expression = {'LabelGroupRules': rule_data_dict['LabelGroupRules']}
            #label_number_dexpression = {'LabelNumberDRules': rule_data_dict['LabelNumberDRules']}


            # number_label_expression, label_number_expression
            #print number_label_expression, number_dynamic_expression  
            #sys.exit()
 
            # logical_expression = { 'MetaRules': ... }
            logical_expression = {'MetaRules': rule_data_dict['MetaRules'] }

            # logical_nodedep_expression = {  'NodeDependentRules': .... 
            logical_nodedep_expression = {'NodeDependentRules': rule_data_dict['NodeDependentRules'] }
              
            # num_analytical_expression = {  'Analytics': ...
            num_analytical_expression = {'Analytics': rule_data_dict['Analytics'] }
              
            # opr_result_expression = {  'FormulaOprResultant': ...
            opr_result_expression = {'FormulaOprResultant': rule_data_dict['FormulaOprResultant'] }
              
            # validation_expressions = {   'userid':  .... } - Direct
            validation_expressions = rule_data_dict['Validation']

            new_validation_expressions = self.group_expression(validation_expressions)

            if 0:
                logical_nodedep_expression={} 
                num_analytical_expression = {}
                opr_result_expression = {}


                del_ar = []
                for k, vdict in logical_expression.items():
                    for v, d in vdict.items():
                        if v in ['PercentageChk3', 'PercentageChk1']: continue
                        del_ar.append((k, v))
                for del_elm in del_ar:
                    del logical_expression[del_elm[0]][del_elm[1]]

                '''  
                #logical_expression ={}
                
                logical_nodedep_expression = {}
                num_analytical_expression = {}
                
                del_ar = []
                for k, vdict in opr_result_expression.items():
                    for v, d in vdict.items():
                        if v == 'U3': continue
                        del_ar.append((k, v))
                for del_elm in del_ar:
                    del opr_result_expression[del_elm[0]][del_elm[1]]
                '''  
            return logical_expression, logical_nodedep_expression, num_analytical_expression,   new_validation_expressions, opr_result_expression, number_label_expression, number_label_dexpression, label_number_expression

        sys.exit() 

        sample_meta_expressions = { 
                                         #'MultiplePHchk1':  [ { 'scope':'COL-RAW', 'parameter':'CellPH', 'filtervalue':'DISTINCT', 'gpoperator':'COUNT', 'elmcondition':'GT 1', 'description':'Multiple PH cannot be in one column'} ], 
                                         'MultiplePHchk2':  [ { 'scope':'COL-RES', 'parameter':'CellPH', 'filtervalue':'DISTINCT', 'gpoperator':'COUNT', 'elmcondition':'GT 1', 'description':'Multiple PH cannot be in one column'} ], 
                                         #'MultiplePHchk3':  [ { 'scope':'COL-REP', 'parameter':'CellPH', 'filtervalue':'DISTINCT', 'gpoperator':'COUNT', 'elmcondition':'GT 1', 'description':'Multiple PH cannot be in one column'} ],

                                         #'MultiplePHchk4':  [ { 'scope':'COL-RAW', 'parameter':'Currency', 'filtervalue':'DISTINCT', 'gpoperator':'COUNT', 'elmcondition':'GT 1', 'description':'Multiple PH cannot be in one column'} ],
                                         'MultiplePHchk5':  [ { 'scope':'COL-RES', 'parameter':'Currency', 'filtervalue':'DISTINCT', 'gpoperator':'COUNT', 'elmcondition':'GT 1', 'description':'Multiple PH cannot be in one column'} ],
                                         #'MultiplePHchk6':  [ { 'scope':'COL-REP', 'parameter':'Currency', 'filtervalue':'DISTINCT', 'gpoperator':'COUNT', 'elmcondition':'GT 1', 'description':'Multiple PH cannot be in one column'} ],

                                         #'MultiplePHchk7':  [ { 'scope':'ROW-RAW', 'parameter':'ValueType', 'filtervalue':'DISTINCT', 'gpoperator':'COUNT', 'elmcondition':'GT 1', 'description':'Multiple PH cannot be in one column'} ],
                                         'MultiplePHchk8':  [ { 'scope':'ROW-RES', 'parameter':'ValueType', 'filtervalue':'DISTINCT', 'gpoperator':'COUNT', 'elmcondition':'GT 1', 'description':'Multiple PH cannot be in one column'} ],
                                         #'MultiplePHchk9':  [ { 'scope':'ROW-REP', 'parameter':'ValueType', 'filtervalue':'DISTINCT', 'gpoperator':'COUNT', 'elmcondition':'GT 1', 'description':'Multiple PH cannot be in one column'} ],
                                          
                                         #'MultiplePHchk10':  [ { 'scope':'ROW-RAW', 'parameter':'Currency', 'filtervalue':'DISTINCT', 'gpoperator':'COUNT', 'elmcondition':'GT 1', 'description':'Multiple PH cannot be in one column'} ],
                                         'MultiplePHchk11':  [ { 'scope':'ROW-RES', 'parameter':'Currency', 'filtervalue':'DISTINCT', 'gpoperator':'COUNT', 'elmcondition':'GT 1', 'description':'Multiple PH cannot be in one column'} ],
                                         #'MultiplePHchk12':  [ { 'scope':'ROW-REP', 'parameter':'Currency', 'filtervalue':'DISTINCT', 'gpoperator':'COUNT', 'elmcondition':'GT 1', 'description':'Multiple PH cannot be in one column'} ],
                                         'c1': [ { 'scope':'ROW-REP', 'parameter':'GVText', 'filtervalue':'*', 'gpoperator':'FOREACH', 'elmcondition':'CONTAINS:L1#PERCENTAGE', 'description':'Mu' }, { 'scope':'ROW-REP', 'parameter':'GVSign', 'filtervalue':'*', 'gpoperator':'FOREACH', 'elmcondition':'EQ Y', 'description':'Mu' } ], 
                                         'c3': [
                                                 { 'scope':'ROW-REP', 'parameter':'CellRes', 'filtervalue':'*', 'gpoperator':'FOREACH', 'elmcondition':'EQ 1', 'description':'Mu' }, 
                                                 { 'scope':'ROW-REP', 'parameter':'CellPH', 'filtervalue':'*', 'gpoperator':'FOREACH:-1', 'elmcondition':'NOTIN-DOCINFOPH', 'description':'Mu' } 
                                               ] , 
                                        
                                         'c4': [
                                                  { 'scope':'ROW-REP', 'parameter':'DocPH', 'filtervalue':'DISTINCT', 'gpoperator':'SET', 'elmcondition':'REMAINDER-FROM-DOCINFOPH', 'description':'Mu' }
                                               ]

                                  }


        sample_nodedepnumerical_expression = {
            'L1':  [ 
                    { 'scope':'REP', 'parameter':'CellPH', 'taxo1':'e50547e3f9291bebc78697051c984ea8', 'taxo2':'656bb794b0486b6b33169e339dd96567', 'elmcondition':'ValueGreaterThan', 'description':'Value of TaxoID1 > TaxoID2' } 
            ],

            'L2': [
                    { 'scope':'REP', 'parameter':'CellPH', 'taxo1':'e50547e3f9291bebc78697051c984ea8', 'taxo2':'656bb794b0486b6b33169e339dd96567', 'elmcondition':'ON OFF', 'description':'Value of TaxoID1 > TaxoID2' } 
            ],

            'L3': [
                    { 'scope':'REP', 'parameter':'CellPH', 'taxo1':'e50547e3f9291bebc78697051c984ea8', 'taxo2':'656bb794b0486b6b33169e339dd96567', 'elmcondition':'OFF ON', 'description':'Value of TaxoID1 > TaxoID2' } 
            ], 


            'L4': [
                    { 'scope':'REP', 'parameter':'CellPH', 'taxo1':'6a308b22efe77377c1a2e2b180b267bc', 'taxo2':'639391e8841fa59854ab2b7d912a583f', 'elmcondition':'ON OFF', 'description':'Value of TaxoID1 > TaxoID2' } 
            ], 

            'L5': [
                    { 'scope':'REP', 'parameter':'CellPH', 'taxo1':'b9adf294eb4f1555079ab628dd2e963b', 'taxo2':'b9adf294eb4f1555079ab628dd2e963b', 'elmcondition':'OFF ON', 'description':'Value of TaxoID1 > TaxoID2' } 
            ] 
        
        }# sample_nodedepnumerical_expression 

        logical_expression = { 'MetaRules': sample_meta_expressions }
        logical_nodedep_expression = { 'NodeDependentRules': sample_nodedepnumerical_expression}

        sample_numericalanalytics_expression = {

            #'ANL1':  [ 
            #        { 'scope':'ROW-REP', 'parameter':'CellPHPeriod', 'taxo':'b9ef7de978fe20e61815230507d1da08', 'filtervalue':'*',   'gpoperator':'POS-SPIKE', 'elmcondition':'GTINC 40', 'description':'Spike length > 40 for taxo id e50547e3f9291bebc78697051c984ea8' } 
            #],   # POS-SPIKE - GTINC, LTINC, GTDEC, LTDEC, ABSGT, ABSLT  

            #'ANL2':  [ 
            #        { 'scope':'ROW-REP', 'parameter':'CellPHPeriod', 'taxo':'*', 'filtervalue':'*',  'gpoperator':'POS-SPIKE', 'elmcondition':'GTDEC 40', 'description':'Spike len > 40 for all' } 
            #],

            'ANL3':  [ 
                    { 'scope':'ROW-REP', 'parameter':'CellPHPeriod', 'taxo':'*', 'filtervalue':'*',  'gpoperator':'ON-OFF', 'elmcondition':'OFF', 'description':'FLAG-OFF' } 
            ],

        }# sample_numericalanalytics_expression 

        num_analytical_expression = { 'Analytics': sample_numericalanalytics_expression }


        sample_opr_res_dict = {

                      'U1': [
                                { 'scope':'REP', 'Ogpoperator':'ON-OFF', 'Rgpoperator':'ON-OFF', 'Ocondition':'OFF' ,'Rcondition':'ON' , 'Description':'One of the operands is null and Resultant is present' }
                       ], # TODOEXPR

                       'U2': [
                                { 'scope':'REP', 'Ogpoperator':'CellRep', 'Rgpoperator':'CellRep', 'Ocondition':'EQ 1' ,'Rcondition':'EQ 1' , 'Description':'Operands and Resulant are from Reported' }
                       ], # TODOEXPR

                       'U3': [
                                { 'scope':'RES', 'Ogpoperator':'CellRes', 'Rgpoperator':'CellRes', 'Ocondition':'EQ 1' ,'Rcondition':'EQ 1' , 'Description':'Operands and Resultant are from Restatement' }
                       ], # TODOEXPR


                      'U4': [
                                { 'scope':'REP', 'Ogpoperator':'ON-OFF', 'Rgpoperator':'checksum', 'Ocondition':'OFF' ,'Rcondition':'GT 4' , 'Description':'One of the operand is null and checksum is greated than 4' }
                       ], # TODOEXPR
                      

                      'U5': [
                                { 'scope':'REP', 'Ogpoperator':'ON-OFF', 'Rgpoperator':'checksum', 'Ocondition':'ON' ,'Rcondition':'EQ 0' , 'Description':'All the operands are present and checksum is zero' }
                       ], # TODOEXPR
        } 

        opr_result_expression = { 'FormulaOprResultant': sample_opr_res_dict } # TODOEXPR 
         

        # ExpressionID Classname SupportingExpressionID  Index   ExpressionStatus    ValidationFlag  Description
        validation_expressions = {  
                       'E1': [  
                                { "classname":"LogicalMeta", "expressionid":'MultiplePHchk1', 'index':0, 'status':"true", 'Description':'Multiple PH in Raw data' },
                                { "validationflag":"Red"}   
                        ],   
                        
                       'E2': [  
                                { "classname":"LogicalMeta", "expressionid":'c2', 'index':0, 'status':"true", 'Description':'Multiple PH in Raw data' },
                                { "classname":"LogicalMeta", "expressionid":'c2', 'index':1, 'status':"false", 'Description':'Multiple PH in Raw data' },
                                { "validationflag":"Red"}   
                        ],
 
                       'E3': [
                                { "classname":"NodeDepnum", "expressionid":'L1', 'index':0, 'status':"true", 'Description':'L1 is true' },
                                { "validationflag":"Red"}   
                       ]

                       # {'Vexp1': [
                       #                  {'status': 'True', 'validationflag': 'Red', 'description': '', 'index': '0', 'expressionid': 'MultiplePHchk1', 'userID': 'Vexp1', 'classname': 'MetaRules'}
                       #            ], 
  
        } # validation_expressions

        return logical_expression, logical_nodedep_expression, num_analytical_expression,   validation_expressions, opr_result_expression



    def search_cleanvalue_exact(self, clean_value, graph_path, repres_flg):
        
        ######################################### EXAMPLE ######################################################
        #graph_path = /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/fe/20_16/ 
        #search_text = "Loans and leases
        ########################################################################################################

         
        fnames = os.listdir(graph_path)
        filenames = []
        if repres_flg == 'RAW':
           for fname in fnames:
               if '_RAW.graphml' in fname:
                   try:
                       d = int(fname.split('_RAW.graphml')[0])
                   except:
                       d = -1
                   if d == -1: continue
                   filenames.append(fname)
        elif repres_flg == 'REP':
           for fname in fnames:
               if '_REP.graphml' in fname:
                   try:
                       d = int(fname.split('_REP.graphml')[0])
                   except:
                       d = -1
                   if d == -1: continue
                   filenames.append(fname)
               
        elif repres_flg == 'RES':
           for fname in fnames:
                if '_RES.graphml' in fname:
                   try:
                       d = int(fname.split('_RES.graphml')[0])
                   except:
                       d = -1
                   if d == -1: continue
                   filenames.append(fname)
        #print 'search_hghtext_exact'           
        #print filenames

        results = []
        for filename in filenames:
            gpath = graph_path + filename 
            G = Graph.Read_GraphML(gpath)
            all_formula_vs_t = G.vs.select(lambda v:v["clean_value"] != '') #graph.vs.select(lambda vertex: vertex.index % 2 == 1)

            all_formula_vs = all_formula_vs_t.select(lambda v:abs(abs(float(v["clean_value"])) - abs(clean_value)) <= 0.0001) #graph.vs.select(lambda vertex: vertex.index % 2 == 1)
            for v in all_formula_vs:
                results.append((v['table_types'], v['grp_id'], v['row_taxo_id'], v['col']))
            del G 
        return results      


    def formula_file_ddict(self, results):
        ddict = {}
        map_res = {}
        for elm in results:
            table_types, grp_id, row_taxo_id, col, hashkey, filename = elm                
            #elm = [ table_types, grp_id, row_taxo_id, col ] 
            if filename not in ddict:
               ddict[filename] = [] 
            ddict[filename].append(elm)
            map_res[hashkey+'#'+col] = elm 
        return ddict, map_res 
             

    def getformulaids(self, results_dict, graph_path):
                                   
        res = {} 
        for filename, vs in results_dict.items():
            ar = filename.split('_') 
            new_ar = ar[:1] + [ 'FORMULA' ] + ar[1:] 
            formulapath = graph_path + '_'.join(new_ar)


            #table_types, grp_id, row_taxo_id, col, hashkey, filename
            hashkeycols_dict = {}
            for v in vs:
                if v[4] not in hashkeycols_dict:
                   hashkeycols_dict[v[4]] = []
                hashkeycols_dict[v[4]].append(v[3])


            hashkeycols = hashkeycols_dict.keys()

            G = Graph.Read_GraphML( formulapath )
            short_vs = G.vs.select(lambda v:v["ID"] in hashkeycols) #graph.vs.select(lambda vertex: vertex.index % 2 == 1)
            
            for sh_v in short_vs:
                if sh_v['colid'] not in hashkeycols_dict[sh_v['ID']]: continue
                fid = sh_v['fid']
                rid = sh_v['rid']
                fname = sh_v['filenames']
                k = fname.split('_')[0] 
                if k+'_'+fid+'_'+rid not in res:
                   res[k+'_'+fid+'_'+rid] = [] 
                res[k+'_'+fid+'_'+rid].append(sh_v['ID']+'#'+ sh_v['colid'])
        return res

    def getadjfilename(self, repres_flg):
        filename = ''
        if 'RAW' == repres_flg:
            filename = 'RAWADJF.graphml'     
        elif 'REP' == repres_flg:
            filename = 'REPADJF.graphml'      
        elif 'RES' == repres_flg:
            filename = 'RESADJF.graphml'      
        if filename == '':
           print 'Some error'
           sys.exit()
        return filename  


    def search_hghtext_exact_between(self, str1, str2, graph_path, repres_flg):
        ########################################################################################################
        #str1 = "Loans and leases"
        #str2 = "Total revenue, net of interest expense"
        #graph_path '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/fe/20_16/', 'RAW')
        ########################################################################################################

        results1 = self.search_hghtext_exact(str1, graph_path, repres_flg)
        results2 = self.search_hghtext_exact(str2, graph_path, repres_flg)

        results1_dict, map1_dict = self.formula_file_ddict(results1)
        results2_dict, map2_dict = self.formula_file_ddict(results2)

        formula_ids_dict1 = self.getformulaids(results1_dict, graph_path)
        formula_ids_dict2 = self.getformulaids(results2_dict, graph_path)
                   
        from_nodes = formula_ids_dict1.keys()
        to_nodes   = formula_ids_dict2.keys() 

        adjfname = self.getadjfilename(repres_flg)
        mpath = graph_path + adjfname 
        G = Graph.Read_GraphML( mpath )
        #short_vs = G.vs.select(lambda v:v["kfidrid"] in from_nodes[:] + to_nodes[:]) #graph.vs.select(lambda vertex: vertex.index % 2 == 1)
        weight=G.es["weight"]         

        filename_map_ddict = {}  
        shortest_paths = {}
        for s in from_nodes:
           for d in to_nodes:
               #res = G.shortest_paths(source=s, target=d, weights=None, mode=OUT) 
               res = G.get_shortest_paths(s, to=d,  mode=OUT, output='vpath')
               for n in res: 
                   if not n: continue
                   temp_short = eval("{}".format(G.vs[n]['name']))
                   highlight1 = map(lambda x:map1_dict[x],  formula_ids_dict1[temp_short[0]])
                   highlight2 = map(lambda x:map2_dict[x],  formula_ids_dict2[temp_short[-1]])

                   for i in range(1, len(n)):
                       int_str =  G.es[G.get_eid(n[i-1], n[i])]['intersection']
                       for elm in int_str.split(':@:'):
                           filename, abs_key, line_key, colid, hashkey = elm.split('#')
                           if filename not in filename_map_ddict:
                              filename_map_ddict[filename] = {}

                           if (s, d) not in filename_map_ddict[filename]:
                              filename_map_ddict[filename][(s, d)] = {}
                           if (temp_short[i-1], temp_short[i]) not in filename_map_ddict[filename][(s, d)]: 
                              filename_map_ddict[filename][(s, d)][(temp_short[i-1], temp_short[i])] = []
                           filename_map_ddict[filename][(s, d)][(temp_short[i-1], temp_short[i])].append(hashkey+'#'+colid)
                   shortest_paths[(s, d)] =  [ temp_short, highlight1, highlight2 ]


        all_results = []
        for fname, vdict in filename_map_ddict.items():
            Gdata = Graph.Read_GraphML(graph_path + fname)
            for (s, d), edge_dict in vdict.items():
                results = shortest_paths[(s, d)]
                ref_dict = {}
                for (n1, n2), hashkeycols in edge_dict.items():
                    ref_dict[(n1, n2)] = []
                    ms = Gdata.vs.select(lambda v:v["hashkeycol"] in hashkeycols)
                    for m in ms:
                        ref_dict[(n1, n2)].append((m["table_types"], m["grp_id"], m["row_taxo_id"], m["col"]))
                results.append(copy.deepcopy(ref_dict))
                all_results.append(results[:])          
            del Gdata  

        return all_results 


    def search_final_results_between(self, search_res1, search_res2, company_id, doc_id):
        graph_path = "/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/tablets_"+str(company_id)+"/"+str(doc_id)+"/" 
        fname_dict = {}
        for search_elm in search_res1:
            search_elm_sp = search_elm.split('#')
            hkeycol = search_elm_sp[0]+'#'+search_elm_sp[1]
            fname = search_elm_sp[2]
            if fname not in fname_dict:
               fname_dict[fname] = []   
            fname_dict[fname].append(hkeycol)

        fname_dict2 = {}
        for search_elm in search_res2:
            search_elm_sp = search_elm.split('#')
            hkeycol = search_elm_sp[0]+'#'+search_elm_sp[1]
            fname = search_elm_sp[2]
            if fname not in fname_dict2:
               fname_dict2[fname] = []   
            fname_dict2[fname].append(hkeycol)

        results_f = {}
        for fname, vs in fname_dict.items():
            ar = fname.split('_') 
            new_ar = ar[:1] + [ 'FORMULA' ] + ar[1:] 
            gpath = graph_path + '_'.join(new_ar)
            G = Graph.Read_GraphML(gpath)
            all_formula_vs = G.vs.select(lambda v:v["hashkeycol"] in vs) #graph.vs.select(lambda vertex: vertex.index % 2 == 1)
            if 'RAW' in '_'.join(ar[1:]):
               sfname = 'RAW' 
            elif 'REP' in '_'.join(ar[1:]):
               sfname = 'REP' 
            elif 'RES' in '_'.join(ar[1:]):
               sfname = 'RES' 
            if sfname not in results_f:
               results_f[sfname] = {}
            for sh_v in all_formula_vs:
                fid = sh_v['fid']
                rid = sh_v['rid']
                fname = sh_v['filenames']
                k = fname.split('_')[0] 
                if k+'_'+fid+'_'+rid not in results_f[sfname]:
                   results_f[sfname][k+'_'+fid+'_'+rid] = []
                results_f[sfname][k+'_'+fid+'_'+rid].append(sh_v['ID']+'#'+ sh_v['colid']+'@'+fname)
            del G

        results2_f = {}
        for fname, vs in fname_dict2.items():
            ar = fname.split('_') 
            new_ar = ar[:1] + [ 'FORMULA' ] + ar[1:]
            gpath = graph_path + '_'.join(new_ar)
            G = Graph.Read_GraphML(gpath)
            all_formula_vs = G.vs.select(lambda v:v["hashkeycol"] in vs) #graph.vs.select(lambda vertex: vertex.index % 2 == 1)
            if 'RAW' in '_'.join(ar[1:]):
               sfname = 'RAW' 
            elif 'REP' in '_'.join(ar[1:]):
               sfname = 'REP' 
            elif 'RES' in '_'.join(ar[1:]):
               sfname = 'RES'
            if sfname not in results2_f:
               results2_f[sfname] = {}
            for sh_v in all_formula_vs:
                fid = sh_v['fid']
                rid = sh_v['rid']
                fname = sh_v['filenames']
                k = fname.split('_')[0] 
                if k+'_'+fid+'_'+rid not in results2_f[sfname]:
                   results2_f[sfname][k+'_'+fid+'_'+rid] = []
                results2_f[sfname][k+'_'+fid+'_'+rid].append(sh_v['ID']+'#'+ sh_v['colid']+'@'+fname)
            del G


        all_results = []
        for sfname , fids1_dict in results_f.items():
            fids2_dict = results2_f[sfname]
            fname = graph_path + sfname+'ADJF.graphml' 

            shortest_paths = {}
            G = Graph.Read_GraphML(fname)

            fids1 = fids1_dict.keys()
            fids2 = fids2_dict.keys()
            filename_map_ddict = {}            

            all_temp_short = []
            all_temp_short_n = []
            for s in fids1:
                for d in fids2:
                    res = G.get_shortest_paths(s, to=d,  output='vpath')
                    for n in res: 
                           if not n: continue
                           temp_short = eval("{}".format(G.vs[n]['name']))
                           all_temp_short.append(temp_short[:])
                           all_temp_short_n.append(n)

            if all_temp_short:
                       del_inds = []
                       for i1, elm1 in enumerate(all_temp_short):
                           s1 = set(elm1)
                           for i2, elm2 in enumerate(all_temp_short):
                               s2 = set(elm2) 
                               if (i1 == i2): continue
                               if (s1.issubset(s2)):
                                  del_inds.append(i1)  

                       for temp_ind, temp_short in enumerate(all_temp_short):

                           if temp_ind in del_inds: continue
                           n = all_temp_short_n[temp_ind]
                           
                           s = temp_short[0]
                           d = temp_short[-1]    

                           fname_dict1 = {}
                           for elm in fids1_dict[s]:
                               if elm.split('@')[1] not in fname_dict1:
                                  fname_dict1[elm.split('@')[1]] = []   
                               fname_dict1[elm.split('@')[1]].append(elm.split('@')[0])
                            

                           fname_dict2 = {}
                           for elm in fids2_dict[d]:
                               if elm.split('@')[1] not in fname_dict2:
                                  fname_dict2[elm.split('@')[1]] = []   
                               fname_dict2[elm.split('@')[1]].append(elm.split('@')[0])
                            

                           highlight1 = []
                           for fname, hcols in fname_dict1.items():
                               Gdata = Graph.Read_GraphML(graph_path + fname)
                               ms = Gdata.vs.select(lambda v:v["hashkeycol"] in hcols)
                               for m in ms:
                                   highlight1.append((m["table_types"], m["grp_id"], m["row_taxo_id"], m["col"]))
                               del Gdata   

                           highlight2 = []
                           for fname, hcols in fname_dict2.items():
                               Gdata = Graph.Read_GraphML(graph_path + fname)
                               ms = Gdata.vs.select(lambda v:v["hashkeycol"] in hcols)
                               for m in ms:
                                   highlight2.append((m["table_types"], m["grp_id"], m["row_taxo_id"], m["col"]))
                               del Gdata   
                                  
                           for i in range(1, len(n)):
                               int_str =  G.es[G.get_eid(n[i-1], n[i])]['intersection']
                               for elm in int_str.split(':@:'):
                                   filename, abs_key, line_key, colid, hashkey = elm.split('#')
                                   if filename not in filename_map_ddict:
                                      filename_map_ddict[filename] = {}

                                   if (s, d) not in filename_map_ddict[filename]:
                                      filename_map_ddict[filename][(s, d)] = {}
                                   if (temp_short[i-1], temp_short[i]) not in filename_map_ddict[filename][(s, d)]: 
                                      filename_map_ddict[filename][(s, d)][(temp_short[i-1], temp_short[i])] = []
                                   filename_map_ddict[filename][(s, d)][(temp_short[i-1], temp_short[i])].append(hashkey+'#'+colid)
                           shortest_paths[(s, d)] =  [ temp_short, highlight1, highlight2 ]
                   
            del G                  

            for fname, vdict in filename_map_ddict.items():
                Gdata = Graph.Read_GraphML(graph_path + fname)
                for (s, d), edge_dict in vdict.items():
                    results = shortest_paths[(s, d)]
                    ref_dict = {}
                    for (n1, n2), hashkeycols in edge_dict.items():
                        ref_dict[(n1, n2)] = []
                        ms = Gdata.vs.select(lambda v:v["hashkeycol"] in hashkeycols)
                        for m in ms:
                            ref_dict[(n1, n2)].append((m["table_types"], m["grp_id"], m["row_taxo_id"], m["col"]))
                    results.append(copy.deepcopy(ref_dict))
                    all_results.append(results[:])         
                del Gdata  

        return all_results 







    def search_final_results(self, search_res, company_id, doc_id):
        graph_path = "/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/tablets_"+str(company_id)+"/"+str(doc_id)+"/" 
        fname_dict = {}
        for search_elm in search_res:
            search_elm_sp = search_elm.split('#')
            hkeycol = search_elm_sp[0]+'#'+search_elm_sp[1]
            fname = search_elm_sp[2]
            if fname not in fname_dict:
               fname_dict[fname] = []   
            fname_dict[fname].append(hkeycol)

        results = []
        for fname, vs in fname_dict.items():
            gpath = graph_path + fname
            G = Graph.Read_GraphML(gpath)
            all_formula_vs = G.vs.select(lambda v:v["hashkeycol"] in vs) #graph.vs.select(lambda vertex: vertex.index % 2 == 1)
            for v in all_formula_vs:
                results.append((v['table_types'], v['grp_id'], v['row_taxo_id'], v['col'], v['hashkey'], fname))
            del G
        return results      


    def search_hghtext_exact(self, search_text, graph_path, repres_flg):
        
        ######################################### EXAMPLE ######################################################
        #graph_path = /root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/fe/20_16/ 
        #search_text = "Loans and leases
        ########################################################################################################

         
        fnames = os.listdir(graph_path)
        filenames = []
        if repres_flg == 'RAW':
           for fname in fnames:
               if '_RAW.graphml' in fname:
                   try:
                       d = int(fname.split('_RAW.graphml')[0])
                   except:
                       d = -1
                   if d == -1: continue
                   filenames.append(fname)
        elif repres_flg == 'REP':
           for fname in fnames:
               if '_REP.graphml' in fname:
                   try:
                       d = int(fname.split('_REP.graphml')[0])
                   except:
                       d = -1
                   if d == -1: continue
                   filenames.append(fname)
               
        elif repres_flg == 'RES':
           for fname in fnames:
                if '_RES.graphml' in fname:
                   try:
                       d = int(fname.split('_RES.graphml')[0])
                   except:
                       d = -1
                   if d == -1: continue
                   filenames.append(fname)
        #print 'search_hghtext_exact'           
        #print filenames

        results = []
        for filename in filenames:
            gpath = graph_path + filename 
            G = Graph.Read_GraphML(gpath)
            all_formula_vs = G.vs.select(lambda v:v["hgh_text"]==search_text) #graph.vs.select(lambda vertex: vertex.index % 2 == 1)
            for v in all_formula_vs:
                results.append((v['table_types'], v['grp_id'], v['row_taxo_id'], v['col'], v['hashkey'], filename))
            del G
        return results      


    def applicator(self, ijson):
        company_name, model_number, deal_id, project_id, company_id, projtype = self.parse_ijson(ijson)
        gpath = '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/'+projtype+'/'+company_id+'/'
        logical_expression, logical_nodedep_expression, num_analytical_expression, validation_expressions, opr_result_expression = self.get_expressions() 

        self.initiate_graph_results(gpath, logical_expression, logical_nodedep_expression, num_analytical_expression, validation_expressions, opr_result_expression)

        lookup_ddict = self.get_lookup_file()
        scope_file_dict = {}
        scope_ar = [ 'COL-RAW', 'COL-REP', 'COL-RES', 'ROW-RAW', 'ROW-REP', 'ROW-RES', 'FORMULA-REP', 'FORMULA-RES', 'FORMULA-RAW' ]
        for scope_elm in scope_ar:
            if 'FORMULA' not in scope_elm:
               file_ddict = self.get_file_scoped_data(gpath, scope_elm)
               scope_file_dict[scope_elm] = copy.deepcopy(file_ddict) # only filenames are store here
            elif 'FORMULA' in scope_elm:
               file_ddict = self.get_file_scoped_data(gpath, scope_elm)
               scope_file_dict[scope_elm] = copy.deepcopy(file_ddict) # only filenames are store here
               #print file_ddict  
            else:
               print 'TO DO: ', scope_elm 
               sys.exit()  


        if logical_expression:
           for sample_expr_type, sample_meta_expressions in logical_expression.items():
                final_result_dict = self.apply_meta_expressions( sample_meta_expressions, scope_file_dict, gpath, lookup_ddict)
                self.update_result_graph(final_result_dict, sample_expr_type, gpath)
                  
             
        if logical_nodedep_expression:                                   
           hkeyfilemap = self.get_hashkey_filemap(gpath)
           for sample_expr_type, sample_meta_expressions in logical_nodedep_expression.items(): 
               final_result_dict = self.apply_numerical_expressions(sample_meta_expressions, scope_file_dict, gpath, hkeyfilemap)
               self.update_result_graph(final_result_dict, sample_expr_type, gpath)

        if num_analytical_expression:
           for sample_expr_type, sample_meta_expressions in num_analytical_expression.items():
               final_result_dict = self.apply_num_analytics(sample_meta_expressions, scope_file_dict, gpath)
               self.update_result_graph(final_result_dict, sample_expr_type, gpath)
 
        if opr_result_expression:
           for sample_expr_type, sample_meta_expressions in opr_result_expression.items():
               final_result_dict = self.apply_oprres_rules(sample_meta_expressions, scope_file_dict, gpath)
               self.update_result_graph(final_result_dict, sample_expr_type, gpath)
         
        self.update_result_validation(validation_expressions, gpath)

    def get_cells_numeq(self, gpath, gpath_neq, scope_file_dict, ttype='RAW', pfactor=0):

        #163_RAW.graphml
        level_fdict = scope_file_dict['COL-'+ttype] 
        table_id_ddict = {}
        for taxo, fdict in level_fdict.items():
            data_fname = fdict['DATA']
            G = Graph.Read_GraphML(gpath+data_fname)
            table_ids = []
            for v in G.vs:
                hashkeycol = v["hashkeycol"]
                #print v
                tab_row_col = v["table_id"]+'^'+v["gv_xml_ids"]
                #row = int(v["row"])
                #col = int(v["col"])
                table_id = v["table_id"] 
                #print ' >>> ', hashkeycol, row, col, table_id
                table_ids.append(table_id)
                if table_id not in table_id_ddict:
                   table_id_ddict[table_id] = {}
                if tab_row_col not in table_id_ddict[table_id]:
                   table_id_ddict[table_id][tab_row_col] = []     
                table_id_ddict[table_id][ tab_row_col].append((hashkeycol , v['group_ids'], (int(v['row']), int(v['col']))  ,  data_fname)) # table_xmlid -> hashkeycol, groupid, r,c , data_fname 
            del G

        #print table_id_ddict.keys()
        #sys.exit() 
        results_dict = {} 
        for ntable_id, row_col_dict in table_id_ddict.items():
            #if ntable_id != '4_37_4': continue
            print ' ntable: ', gpath_neq + ntable_id + ".graphml"                 
            try: 
                G = Graph.Read_GraphML(gpath_neq+ntable_id+".graphml")
            except:
                print "no number equality", gpath_neq+ntable_id+".graphml"
                continue
            for edge in G.es.select(factor=pfactor):
                #print '========================================='
                #print 'ntable_id: ', ntable_id 
                if (G.vs[edge.source]["name"] in row_col_dict):
                    if 1:
                      # debug with muthu
                      source_hkeys = row_col_dict[G.vs[edge.source]["name"]]
                      target = G.vs[edge.target]["name"] 
                      target_table = target.split('^')[0]
                      try:
                          target_hkeys = table_id_ddict[target_table][target] 
                      except:
                          continue

                    all_ar = []
                    for source_hkey in source_hkeys:
                        for target_hkey in target_hkeys:
                            ar = [ source_hkey, ntable_id , G.vs[edge.target]["name"],  target_hkey, target_table,  G.vs[edge.source]["name"] ]
                            all_ar.append(ar[:])

                    if (ntable_id, target_table) not in results_dict:   
                       results_dict[(ntable_id, target_table)]= []  
                    results_dict[(ntable_id, target_table)] += all_ar[:]

                elif (G.vs[edge.target]["name"] in row_col_dict):
                    if 1:
                      #print ' chk here ', G.vs[edge.target]["name"], ' ** ', G.vs[edge.source]["name"]
                      # debug with muthu
                      source_hkeys = row_col_dict[G.vs[edge.target]["name"]]
                      target = G.vs[edge.source]["name"] 
                      target_table = target.split('^')[0]
                      try:  
                          target_hkeys = table_id_ddict[target_table][target]
                      except:
                          continue

                    all_ar = []
                    for source_hkey in source_hkeys:
                        for target_hkey in target_hkeys:
                            ar = [ source_hkey, ntable_id , G.vs[edge.target]["name"],  target_hkey, target_table,  G.vs[edge.source]["name"] ]
                            all_ar.append(ar[:])

                    if (ntable_id, target_table) not in results_dict:   
                       results_dict[(ntable_id, target_table)]= []  
                    results_dict[(ntable_id, target_table)] += all_ar[:]
            del G  


        return results_dict  

                    
    def get_label_match(self, results_dict, taxo_match_type, gpath_label):
        
        for table_pair, table_result in results_dict.items():
            G1 = Graph.Read_GraphML(gpath_label+table_pair[0]+".graphml")
            G2 = Graph.Read_GraphML(gpath_label+table_pair[1]+".graphml")
            print gpath_label+table_pair[0]+".graphml"
            print gpath_label+table_pair[1]+".graphml" 
            for result in table_result:
                t1 = result[2]
                t2 = result[5]
                print [ t1, t2 ] 
                vs1 = G1.vs.select(lambda v:v["node"]==t1)
                vs2 = G2.vs.select(lambda v:v["node"]==t2)

                if not vs1: continue 
                if not vs2: continue 
                print 'results1: ', vs1
                print 'results2: ', vs2       
            
                sys.exit() 
            del G1
            del G2
        #sys.exit()   


    def taxonomy_equality(self, t1, t2, mtype):
        if mtype == "EQUAL":
           if t1 == t2: return 1, t1

        else:
           t1_sp = t1.split('.')
           t2_sp = t2.split('.')
              
           m = min([len(t1_sp), len(t2_sp)])
           m_ar = []
           for i in range(0, m):
               if t1_sp[i] == t2_sp[i]:
                  m_ar.append(t1_sp[i])
               else:
                  break
           if m_ar:
              return 1, '.'.join(m_ar)  

        return 0, ''          

    def applicator_tablet(self, companyid, doc_id):
        #company_name, model_number, deal_id, project_id, company_id, projtype = self.parse_ijson(ijson)
        #gpath = '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/'+companyid+'/'+doc_id+'/'
        gpath = '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/'+companyid+'/'+doc_id+'/tablets/'
        gpath_neq = '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/'+companyid+'/'+doc_id+'/numeq/'
        gpath_label = '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/'+companyid+'/'+doc_id+'/label/'
        gpath_numtolabel = '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/'+companyid+'/'+doc_id+'/numtolabel/'
        gpath_labeltonum =  '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/'+companyid+'/'+doc_id+'/labeltonum/'

       
        logical_expression, logical_nodedep_expression, num_analytical_expression, validation_expressions, opr_result_expression, number_label_expression, number_label_dexpression, label_number_expression = self.get_expressions() 

        if 0:
           # for validation  
           self.initiate_graph_results(gpath, logical_expression, logical_nodedep_expression, num_analytical_expression, validation_expressions, opr_result_expression) # for validation
        #print 'post initiate_graph_results'  
        #sys.exit()

        if 1:
           self.initiate_graph_results_num_to_label(gpath,  gpath_numtolabel, number_label_expression, number_label_dexpression, label_number_expression)
              

        lookup_ddict = self.get_lookup_file()
        scope_file_dict = {}
        scope_ar = [ 'COL-RAW', 'COL-REP', 'COL-RES', 'ROW-RAW', 'ROW-REP', 'ROW-RES', 'FORMULA-REP', 'FORMULA-RES', 'FORMULA-RAW' ]
        for scope_elm in scope_ar:
            if 'FORMULA' not in scope_elm:
               file_ddict = self.get_file_scoped_data(gpath, scope_elm)
               scope_file_dict[scope_elm] = copy.deepcopy(file_ddict) # only filenames are store here
            elif 'FORMULA' in scope_elm:
               file_ddict = self.get_file_scoped_data(gpath, scope_elm)
               scope_file_dict[scope_elm] = copy.deepcopy(file_ddict) # only filenames are store here
               #print 'FORMULA: ', file_ddict  
            else:
               print 'TO DO: ', scope_elm 
               sys.exit()  

        if 1: # uncomment - later
           # number-to-label expression
           # apply number_label_expression then apply number_label_dexpression  
           if number_label_expression:
              for expr_type, expressions in number_label_expression.items():
                  print '==============================================='
                  print 'expr_type: ', expr_type
                  print 'expressions: ', expressions 
                  final_result_dict = self.apply_number_label_expressions(expressions, scope_file_dict, gpath, gpath_neq, gpath_label)
                  self.update_result_graph_group(final_result_dict, expr_type, gpath_numtolabel)
           #sys.exit()        
        if 1:  
           if number_label_dexpression:
              for expr_type, expressions in number_label_dexpression.items():
                  print '==============================================='
                  print 'dexpr_type: ', expr_type
                  print 'dexpressions: ', expressions
                  final_result_dict = self.apply_number_label_dexpressions(expressions, scope_file_dict, gpath_numtolabel, gpath)
                  self.update_result_graph_group(final_result_dict, expr_type, gpath_numtolabel)
           print 'Done after'   
           #sys.exit()  

 
        if 1:
           if label_number_expression:
              for expr_type, expressions in label_number_expression.items():
                  print '=============================================='
                  print 'expr_type: ', expr_type
                  print 'expressions: ', expressions   
                  all_results = self.apply_label_number_expression(expressions, scope_file_dict, gpath_label, gpath_labeltonum, gpath, companyid, doc_id)
           sys.exit()                                 

        #num_results_dict = self.get_cells_numeq(companyid, doc_id, gpath, gpath_neq, scope_file_dict, "RAW", 1)
        #self.get_label_match(num_results_dict, "EQUAL", gpath_label)
 
        sys.exit() 

        #print scope_file_dict
        if logical_expression:
           for sample_expr_type, sample_meta_expressions in logical_expression.items():
                final_result_dict = self.apply_meta_expressions( sample_meta_expressions, scope_file_dict, gpath, lookup_ddict)
                #print final_result_dict['138_RAW.graphml']
                #sys.exit() 
                self.update_result_graph(final_result_dict, sample_expr_type, gpath)
                  
        sys.exit()
        print 'done scope_ar'
        if logical_nodedep_expression:                                   
           hkeyfilemap = self.get_hashkey_filemap(gpath)
           for sample_expr_type, sample_meta_expressions in logical_nodedep_expression.items(): 
               final_result_dict = self.apply_numerical_expressions(sample_meta_expressions, scope_file_dict, gpath, hkeyfilemap)
               self.update_result_graph(final_result_dict, sample_expr_type, gpath)

        if num_analytical_expression:
           for sample_expr_type, sample_meta_expressions in num_analytical_expression.items():
               final_result_dict = self.apply_num_analytics(sample_meta_expressions, scope_file_dict, gpath)
               self.update_result_graph(final_result_dict, sample_expr_type, gpath)
 
        if opr_result_expression:
           for sample_expr_type, sample_meta_expressions in opr_result_expression.items():
               final_result_dict = self.apply_oprres_rules(sample_meta_expressions, scope_file_dict, gpath)
               self.update_result_graph(final_result_dict, sample_expr_type, gpath)

        self.update_result_validation(validation_expressions, gpath)

        print 'done update_result_validation' 
        #sys.exit()     


    def display_applicator_tablet(self, companyid, doc_id):
        gpath = '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/'+companyid+'/'+doc_id+'/tablets/'
        gpath_neq = '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/'+companyid+'/'+doc_id+'/numeq/'
        gpath_label = '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/'+companyid+'/'+doc_id+'/label/'
        gpath_numtolabel = '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/'+companyid+'/'+doc_id+'/numtolabel/' # results storage path

        logical_expression, logical_nodedep_expression, num_analytical_expression, validation_expressions, opr_result_expression, number_label_expression, number_label_dexpression, label_number_expression = self.get_expressions()

        html_display_path = '/var/www/html/avinash/label_number/'+companyid+'/'+doc_id+'/'

        os.system('rm -rf '+html_display_path)
        os.system('mkdir -p '+html_display_path)

        if 1:#number_label_expression:
              all_keys = []   
              for expr_type, expressions in number_label_expression.items():
                  for exp_id, expr_ar in expressions.items():
                      for exp_ind , expr in enumerate(expr_ar):
                          mykey = expr_type+'_'+exp_id+'_'+str(exp_ind) 
                          #print mykey , expr_ar 
                          all_keys.append(mykey)
              #print all_keys
              for all_key in all_keys:
                  mresfname = html_display_path+all_key+'/'
                  os.system('mkdir -p '+mresfname)
                  res_files = os.listdir(gpath_numtolabel)
                  mdict = {}  
                  for rfname in res_files:
                      rpath = gpath_numtolabel + rfname
                      #print 'mmmmmrpath: ', rpath
                      G = Graph.Read_GraphML(rpath)
                      for v in G.vs:
                          if v[all_key]: 
                             #print 'table_row_cols: ', v['table_row_cols'], ' all_key: ', v[all_key]
                             #table_row_col = v['']
                             row = int(v['row'])
                             col = int(v['col'])
                             row_col = (row, col)  
                              
                             hashkeycol = v['hashkey'] + '#' + str(int(v['col']))

                             #print ' hashkeycol: ', hashkeycol   
                             G1 = Graph.Read_GraphML(gpath+rfname)
                             #print ' otherfname: ', gpath+rfname
                             nvs = G1.vs.select(lambda v:v["hashkeycol"]==hashkeycol) #graph.vs.select(lambda vertex: vertex.index % 2 == 1)
                             num = ''
                             gpid = ''  
                             table_id = ''
                             for n in nvs:
                                 num = n['gv_text']
                                 gpid = n['group_ids']
                                 table_id = n['table_id'] 
                             ar = [ table_id, row_col, num, gpid ]

                             del G1

                             fname_hcol_dict = {}   
                             for elm in v[all_key].split('$$'):
                                 hcol2, fname2, txn_level2, pair_ids  = elm.split('@')
                                 if gpath+fname2 not in fname_hcol_dict:
                                    fname_hcol_dict[gpath+fname2] = []
                                 fname_hcol_dict[gpath+fname2].append(hcol2)


                             for newfpath, newhcols in fname_hcol_dict.items(): 
                                 G1 = Graph.Read_GraphML(newfpath)
                                 nvs = G1.vs.select(lambda v:v["hashkeycol"] in newhcols) 
                                 for n in nvs:
                                     num1 = n['gv_text']
                                     gpid1 = n['group_ids']
                                     table_id1 = n['table_id']
                                     row1 = int(n['row'])
                                     col1 = int(n['col'])

                                     new_ar = ar[:] + [ table_id1, (row1, col1), num1, gpid1 ]
                                     if ar[0] not in mdict:
                                        mdict[ar[0]] = []
                                     mdict[ar[0]].append(new_ar)
                                 del G1
                      del G

                  if mdict: 
                     headers = [ 'TableID1', 'rc1', 'GvText1', 'GroupId1', 'TableID2', 'rc2', 'GvText2', 'GroupId2' ]
                     for k, vs in mdict.items():
                         htmlpath = mresfname+k+'.html'
                         f = open(htmlpath, 'w')
                         f.write('<html>')   
                         f.write('<table border=1>') 
                         f.write('<tr>')
                         for h in headers:
                             f.write('<td bgcolor=yellow><b>'+h+'</b></td>')  
                         f.write('</tr>')  
 
                         for v in vs:
                             f.write('<tr>')
                             for e in v:
                                 f.write('<td>'+str(e)+'</td>')
                             f.write('</tr>')
                                        
                         f.write('</table>')
                         f.write('</html>')   
                         f.close()    
                         print 'done writing in ', htmlpath    
                         #sys.exit()              
                      
        if 1:#number_label_dexpression:
              all_keys = []   
              for expr_type, expressions in number_label_dexpression.items():
                  for exp_id, expr_ar in expressions.items():
                      for exp_ind , expr in enumerate(expr_ar):
                          mykey = expr_type+'_'+exp_id+'_'+str(exp_ind) 
                          #print mykey , expr_ar 
                          all_keys.append(mykey)
              #print all_keys
              #sys.exit()  
              for all_key in all_keys:
                  mresfname = html_display_path+all_key+'/'
                  os.system('mkdir -p '+mresfname)
                  res_files = os.listdir(gpath_numtolabel)
                  #print gpath_numtolabel  
                  #print res_files
                  #sys.exit() 
                            
                  for rfname in res_files:
                      gpdict = {} 
                      rpath = gpath_numtolabel + rfname
                      G = Graph.Read_GraphML(rpath)
                      #print ' --- rpath: ', rpath 
                      for v in G.vs:
                          if v[all_key]: 
                             hashcolkey1 = v['hashcolkeys']
                             G1 = Graph.Read_GraphML(gpath+rfname)
                             nvs = G1.vs.select(lambda v:v["hashkeycol"]==hashcolkey1) #graph.vs.select(lambda vertex: vertex.index % 2 == 1)
                             num1 = '' 
                             for n in nvs:
                                 num1 = n['gv_text']
                             del G1    
                             
                             for elm in v[all_key].split('$$'):
                                 #print elm.split('@') 
                                 hashcolkey2, fname2, txn_level2, pair_ids, row_col, gp_id  = elm.split('@')
                                   
                                 pair_ids_sp = pair_ids.split('*')
                                 #print pair_ids_sp
                                 table_id1 = pair_ids_sp[0].split('^')[0]
                                 table_id2 = pair_ids_sp[1].split('^')[0]
                                 #print [ table_id1, table_id2 ]
                                 row_col1 = eval(pair_ids_sp[2])  
                                 row_col2 = eval(pair_ids_sp[3]) 
                                 #print [ row_col1, row_col2 ]  
                                 gp_id1 = pair_ids_sp[4].replace('A', '@') 
                                 gp_id2 = pair_ids_sp[5].replace('A', '@') 
                      
                                 #print '=========================================='  
                                 #print 'group_id: ', gp_id
                                 #print elm            
                                 #print [ gp_id1, gp_id2 ]

                                 G1 = Graph.Read_GraphML(gpath+fname2)
                                 nvs = G1.vs.select(lambda v:v["hashkeycol"]==hashcolkey2) #graph.vs.select(lambda vertex: vertex.index % 2 == 1)
                                 num2 = '' 
                                 for n in nvs:
                                     num2 = n['gv_text']
                                 del G1    
 
                                 #print [ num1, num2 ]  

                                 if table_id1 not in gpdict:
                                    gpdict[table_id1] = {}
                                 if gp_id not in gpdict[table_id1]:
                                    gpdict[table_id1][gp_id] = []  
                                 gpdict[table_id1][gp_id].append((table_id1, table_id2, row_col1, row_col2, gp_id1,  gp_id2, num1, num2, txn_level2, row_col))
                                   
                      del G             
                      if gpdict:
                          for table_id, val_dict in gpdict.items():
                              #print ' table_id: ', table_id
                              f = open(mresfname+table_id+'.html', 'w')
                              f.write('<html>')
                              for gp, vals in val_dict.items():
                                  print ' --- ', gp
                                  f.write('<hr>')
                                  f.write('Groupid: '+str(gp))
                                  f.write('<table border=1>') 
                                  headers =    ['Tableid1', 'Tableid2', 'RowCol1', 'RowCol2', 'GpID1', 'GpID2', 'Num1', 'Num2', 'TaxoLvl', 'RowCol']
                                  f.write('<tr>')
                                  for h in headers:
                                      f.write('<td bgcolor=yellow><b>'+h+'</b></td>')
                                  f.write('</tr>')
                                  for val in vals:
                                      #print '         ++++ ', val
                                      f.write('<tr>') 
                                      for val_elm in val:
                                          f.write('<td>'+str(val_elm)+'</td>') 
                                      f.write('</tr>') 
                                  f.write('</table>')
                              f.write('</html>')
                              f.close()
                              print 'done filewriting: ', mresfname+table_id+'.html'
                        
          


if __name__ == '__main__':
    # python raw_preview_builder_data_app.py 20_16 '{"project_name":"Key Banking Capital and Profitability Figures"}'
    
    obj = Validate()
    #obj.applicator_tablet('1117', '5131')
    obj.applicator_tablet('1604', '1')
    #obj.display_applicator_tablet('1604', '4')

    sys.exit() 
    if  0:
        obj = Validate()
        try:
            ijson   = json.loads(sys.argv[1])
        except:
            deal_id = sys.argv[1]
            ijson   = m_obj.read_company_info({"cids":[deal_id.split('_')[1]]})[deal_id]
            if len(sys.argv) > 2:
                tmpjson = json.loads(sys.argv[2])
                ijson.update(tmpjson)
        obj.applicator(ijson)

    if 0: 
        obj = Validate()
        #results = obj.search_hghtext_exact("Loans and leases", '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/fe/20_16/', 'RAW')
        #print results   
        #results = obj.search_cleanvalue_exact(50886, '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/fe/20_16/', 'RAW')
        #print results   

        ################## FOR MUTHU ##################################################################### 
        #search_res = ['c6457677c8046b632469b49ca86246b8#1#1_RAW.graphml#HGH', 'c6457677c8046b632469b49ca86246b8#2#1_RAW.graphml#HGH', 'c6457677c8046b632469b49ca86246b8#1#1_RAW.graphml#VGH' ]
        #search_res = ["c68e2eab17c593307af3ecf452bd1713#1#80_RAW.graphml#HGH","c68e2eab17c593307af3ecf452bd1713#2#80_RAW.graphml#HGH","c68e2eab17c593307af3ecf452bd1713#3#80_RAW.graphml#HGH"] 

        #ar = obj.search_final_results(search_res, '1117', '5131' )
        #print ar  
        ################## FOR MUTHU ##################################################################### 

        #print results  
        #results = obj.search_hghtext_exact_between("Loans and leases", "Total revenue, net of interest expense", '/root/databuilder_train_ui/tenkTraining/Data_Builder_Training_Copy/pysrc/graphlbv/fe/20_16/', 'RAW')
        #for result in results:
        #    print result
    
        #ar1 = ["d438be7cec00668a519a297835f44c52#1#1_RAW.graphml#HGH","d438be7cec00668a519a297835f44c52#2#1_RAW.graphml#HGH"]
        #ar2 = ["c8b2c602d0b758beddedabb2e0c5194f#1#2_RAW.graphml#HGH","c8b2c602d0b758beddedabb2e0c5194f#2#2_RAW.graphml#HGH","c8b2c602d0b758beddedabb2e0c5194f#3#2_RAW.graphml#HGH"] 

        ar1 = ["d438be7cec00668a519a297835f44c52#1#1_RAW.graphml#HGH","d438be7cec00668a519a297835f44c52#2#1_RAW.graphml#HGH"]
        ar2 = ["c8b2c602d0b758beddedabb2e0c5194f#1#2_RAW.graphml#HGH","c8b2c602d0b758beddedabb2e0c5194f#2#2_RAW.graphml#HGH","c8b2c602d0b758beddedabb2e0c5194f#3#2_RAW.graphml#HGH"]  



        ar1 = ["6401648d79511309293eb493b8c12eea#1#4_RAW.graphml#HGH","6401648d79511309293eb493b8c12eea#2#4_RAW.graphml#HGH","21341db71a0d58e94b276207a84bb22a#1#59_RAW.graphml#HGH","21341db71a0d58e94b276207a84bb22a#2#59_RAW.graphml#HGH","21341db71a0d58e94b276207a84bb22a#3#59_RAW.graphml#HGH"]
        ar2 = ["c8b2c602d0b758beddedabb2e0c5194f#1#102_RAW.graphml#HGH","c8b2c602d0b758beddedabb2e0c5194f#2#102_RAW.graphml#HGH","c8b2c602d0b758beddedabb2e0c5194f#3#102_RAW.graphml#HGH"] 
        result = obj.search_final_results_between(ar1, ar2, '1117', '5131')  
        for r in result:
            print r  

