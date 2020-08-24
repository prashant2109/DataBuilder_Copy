import os, sys, sqlite3

class HRA_info():
    def __init__(self):
        pass 
    
    def cgi_data(self, doc_id, tab_id):
        cmd = 'cd /var/www/cgi-bin/ShareHolders/tasproject_1/src/; python service_display2_hrb_replica2_muthu.py %s %s; cd -'%(doc_id, tab_id) 
        print cmd
        os.system(cmd)
        return 'done'
    
    def taxo_tran_table_data(self, doc_id, tab_id):
        pass
    


if __name__ == '__main__':
    pass 
    
