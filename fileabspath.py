import os, sys
import re
import platform
class fileabspath(object):
    def get_abs_file_path(self, filename):
        abs_file_path = os.path.abspath(filename)
        return abs_file_path

