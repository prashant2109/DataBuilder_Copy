# -*- coding: utf-8 -*-
import re

def replace_notes_at_end(input_str):
    while True:
        a =  re.search(r'(\(\s*(note|Note|NOTE)*\s*\d+\s*\))$', input_str)
        if not a:
            break 
        input_str = re.sub(r'(\(\s*(note|Note|NOTE)*\s*\d+\s*\))$', "", input_str)
        input_str = input_str.strip()
    return input_str

if __name__ == "__main__":
    st = "Ho note(12) nnegowda (Note12) ( note 14) (14) (note12)"
    print "input_str====>", replace_notes_at_end(st)
      
        
