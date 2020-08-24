def get_capitalized_taxo_lbl(data):
    data = data.strip()
    f_alpha_charcter_idx = -1
    for i, d in enumerate(data):
        if d.isalpha():
            f_alpha_charcter_idx = i
            break
    if f_alpha_charcter_idx >= 0:
        data = data[f_alpha_charcter_idx].upper() + data[f_alpha_charcter_idx+1:]
    else:
        data = ''
    return data 


#st = "!g"
#print [get_capitalized_taxo_lbl(st)]
