import os, platform, re
class filePathAdj:
    def get_file_path(self, filename):    
        os_type = platform.system()
        ##For Linux, use same file or no change in filename
        if os_type.lower() == "linux":
            return filename
        ##For Exe, filepointer is passed from system buffer , relative name will be same as baseName of the filename
        elif "_MEIPASS2" in os.environ.keys():
            return filename
            file_sp = os.path.split(filename)
            win_path = os.path.join(os.environ["_MEIPASS2"], file_sp[1])
            return win_path
        ##For Windows(Default Windows), filename will be absolute path on windows
        else:
            file_sp = os.path.split(filename)
            fullFname = os.path.abspath(filename)    
            #fullFname = re.sub('\\', '\\\\', fullFname) 
            fullFname = "\\\\".join(fullFname.split("\\")) 
#            fullFname = re.sub(r'\\b', '\\\\b', fullFname) 
#            fullFname = re.sub(r'\\a', '\\\\a', fullFname) 
#            fullFname = re.sub(r'\\f', '\\\\f', fullFname) 
            f = open('file_path.txt', 'a')
            f.write("('"+file_sp[1]+"', '"+fullFname+"', 'DATA'), ")
            f.close()  
            #print fullFname
            return fullFname   
