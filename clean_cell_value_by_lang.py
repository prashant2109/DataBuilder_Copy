import locale
from dateutil.parser import *
from dateutil.relativedelta import *
import os, sys
import urllib2
from decimal import *

class CleanCellValueByLang(object):
    def __init__(self):
        pass


    def unicodeToHTMLEntities(self, text):
        """Converts unicode to HTML entities.  For example '&' becomes '&amp;'."""
        text = cgi.escape(text).encode('ascii', 'xmlcharrefreplace')
        return text

    def convertNonAsciiToHTMLEntities(self, text):
        text = text.replace('&amp;', '&')
        text = text.replace('EUR', '')
        text = text.replace('"', '')
        text = text.replace("'", "")
        text = text.replace("_", "")
        text = text.replace("&#8213;", "")
        new_txt = ""
        error_flg = 0
        find_entities = {}
        for x in text:
            if ord(x)==ord('&'):
               new_txt += '&amp;'
            elif ord(x) < 128:
                new_txt += x
            else:
                flg = 0
                try:
                    elm =  self.unicodeToHTMLEntities(x)
                except:
                    flg = 1
                    elm = '&#'+str(ord(x))+';'
                if flg:
                   find_entities[elm] = 0
                new_txt += elm #self.unicodeToHTMLEntities(x)
        new_txt = new_txt.replace('&amp;#', '&#')
        new_txt = new_txt.replace('&amp;quot;', '&quot;')
        new_txt = new_txt.replace('&#160;', ' ')
        new_txt = new_txt.replace('&#165;', '')
        new_txt = new_txt.replace('&#8211;', '-')
        new_txt = new_txt.replace('&#8212;', '-')
        new_txt = new_txt.replace('&#8217;', "'")
        new_txt = new_txt.replace('&#8220', '"')
        new_txt = new_txt.replace('&#8221', '"')
        new_txt = new_txt.replace('&#8212;', "-")
        new_txt = new_txt.replace('&#8722;', '-')
        new_txt = new_txt.replace('__', '')
        new_txt = new_txt.replace('-,--', '')
        new_txt = new_txt.replace('&#8202;', '')
        new_txt = new_txt.replace('&#195;&#188;', '&#252;')
        new_txt = new_txt.replace('&#195;&#156;', '&#220;')
        new_txt = new_txt.replace('&#204;', '')

        new_txt = ' '.join(new_txt.strip().split())
        cp1252_to_unicode_dict = {'&#92;':'&#2019;', '&#145;':'&#8216;', '&#146;':'&#8217;', '&#147;':'&#8220;', '&#148;':'&#8221;', '&#149;':'&#8226;', '&#150;':'&#8211;', '&#151;':'&#8212;', '&#152;':'&#732;', '&#153;':'&#8482;','&#195;&#164;':'&#228;', '&#195;&#182;':'&#246;'}
        for k, v in cp1252_to_unicode_dict.items():
            new_txt = new_txt.replace(k, v)

        new_txt = str(' '.join(new_txt.split()))
        new_txt = new_txt.replace('EUR', '')
        new_txt = new_txt.replace('"', '')
        new_txt = new_txt.replace("'", "")
        new_txt = new_txt.replace("_", "")
        new_txt = "".join([each for each in new_txt if not each.isalpha()])
        return new_txt


    def get_clean_number_by_lang(self, lang, txt):
        txt = str(txt)

        #print lang
        #print 'before: ', txt
        txt = ''.join(map(lambda x:x.strip(), txt.split(' ')))
        flg = 1
        #lst = ['-', '$', '%']
        lst = ['$', '%', '*']
        while flg:
            flg = 0
            for x in lst:
                if x in txt:
                   txt = txt.replace(x, '')
                   txt = txt.strip()
                   flg = 1 
        txt_ar = txt.split('.')
        if lang == 'E':
            txt = txt.replace(',', '')
        if lang == 'G':
           txt = txt.replace('.', '')
           txt = txt.replace(',', '.')
        txt = txt.strip()
        if '(' and ')' in str(txt):
            txt = txt.replace('(', '').replace(')', '')
            txt = '-%s' % str(txt)
        else:
            txt = str(txt)
        txt = txt.replace('&#8364;', '')
        if(len([c for c in txt if c.isdigit()])):
             return txt
        else: return ""

    def __isfloat_int(self, taxo_value):
        #print taxo_value, "<br>" 
        taxo_value = taxo_value.strip()
        if '-' in taxo_value or '*' in taxo_value:
            return taxo_value
        if '.' in taxo_value:
           if len(taxo_value) >= 2:
              #print 'before_making_float: ', taxo_value, '<br>'
              #if taxo_value: taxo_value = float(taxo_value)
              if taxo_value: taxo_value = Decimal(taxo_value)
              #print 'after_making_float: ', taxo_value, "<br>"
           else:
              taxo_value = ""

        elif taxo_value:
            taxo_value = int(taxo_value)
        return taxo_value

    def clean_cell_value_without_currency(self, taxo_value_ar):
        if len(taxo_value_ar) == 1:
            taxo_value = taxo_value_ar[0][0].strip().replace(" ", '')
            taxo_value = urllib2.unquote(taxo_value)
            taxo_value = taxo_value.strip().replace(" ", "")
            par_taxo_md_dict = taxo_value_ar[0][2]
            taxo_md_dict = dict((k.lower(), v) for k,v in par_taxo_md_dict.iteritems())
            doc_lang = taxo_md_dict.get('model', 'ENGLISH')
            taxo_value = self.convertNonAsciiToHTMLEntities(taxo_value)
            if doc_lang == "GERMAN":
               taxo_value = taxo_value.replace('.', ',')
            return taxo_value
        else:
            value_li = []
            for i,each in enumerate(taxo_value_ar):
               each = list(each)
               par_taxo_md_dict = each[2]
               each[0] = urllib2.unquote(each[0])
               each[0] = self.convertNonAsciiToHTMLEntities(each[0])
               taxo_md_dict = dict((k.lower(), v) for k,v in par_taxo_md_dict.iteritems())
               doc_lang = taxo_md_dict.get('model', 'ENGLISH')
               if doc_lang == "ENGLISH":
                 each[0] =each[0].replace(',', '')
               else:
                 each[0] = each[0].replace('.', '')
                 each[0] = each[0].replace(',', '.')
               value_li.append(each[0])
            value_li = [each.strip().replace(" ", "").replace('-', '') for each in value_li]
            value_li = [each for each in value_li if each]
            value_li = [int(each) for each in value_li]
            taxo_value = reduce(lambda x,y: x+y, value_li)
            return taxo_value

    def clean_cell_value(self, taxo_value, md_data_dict):
        doc_unit_dict = {'th':'1000', 'mn':'1000000', 'bn':'1000000000', '3':'1000', '6':'1000000', '9':'1000000000'}
        doc_lang_dict = {'german': 'G', 'english': 'E'}
        neg_flag = 0 
        taxo_value = taxo_value.strip().replace(" ", "")
        taxo_value = urllib2.unquote(taxo_value)
        taxo_value = self.convertNonAsciiToHTMLEntities(taxo_value)
        taxo_value = taxo_value.strip().replace(" ", "")
        md_data_dict = dict((k.lower(), v) for k,v in md_data_dict.iteritems())
        doc_lang = md_data_dict.get('model', 'ENGLISH').lower()
        doc_unit = md_data_dict.get('units', 'ACTUAL').lower()
        taxo_value = self.get_clean_number_by_lang(doc_lang_dict.get(doc_lang, 'G'), taxo_value)
        #print "taxo_value==========>", doc_lang, taxo_value 
        if '-' in taxo_value:
            taxo_value = taxo_value.replace('-', '')
            neg_flag = 1

        if '(' in taxo_value:
            taxo_value = taxo_value.replace('(', '')
            taxo_value = taxo_value.replace(')', '')
            neg_flag = 1
    
        #print neg_flag, "<br>"
        first_precision = 0
        if '.' in str(taxo_value):
            first_precision = len(str(taxo_value).split('.')[1])
        #print "first_precision====>", first_precision , "<br>"
        #print taxo_value
        taxo_value = self.__isfloat_int(taxo_value)
        #print "after float ====>", taxo_value , "<br>"
        taxo_value = taxo_value * int(doc_unit_dict.get(doc_unit, 1))
        #print "unit====>", int(doc_unit_dict.get(doc_unit, 1)), "<br>"
        #print "after mul ====>", taxo_value , "<br>"
        if '.' in str(taxo_value):
            final_precision = len(str(taxo_value).split('.')[1])
            #print "final_precision====>", final_precision , "<br>"
            #print "taxo_value before round====>", taxo_value , "<br>"
            if (first_precision > final_precision):
                precision_value = '0'*(first_precision-final_precision)
                #print "precision value ====>", precision_value, "<br>"
                taxo_value = str(taxo_value) + str(precision_value)
                #print "taxo value ====>", taxo_value, "<br>"
            else:
                print "taxo_value=====>", taxo_value
                taxo_value = round(taxo_value, first_precision)
            
        if taxo_value == '': return taxo_value
        taxo_value = str(taxo_value)      
        if neg_flag:
           taxo_value = '-%s' % (taxo_value)
        taxo_value = taxo_value.replace(",","")
        return taxo_value

    def clean_cell_value_old(self, taxo_value_ar):
        doc_unit_dict = {'TH':'1000', 'MN':'1000000', 'BN':'1000000000', '3':'1000', '6':'1000000', '9':'1000000000'}
        doc_lang_dict = {'GERMAN': 'G', 'ENGLISH': 'E'}
        neg_flag = 0 
        if len(taxo_value_ar) == 1:
            taxo_value = taxo_value_ar[0][0].strip().replace(" ", "")
            doc_unit = taxo_value_ar[0][1]
            taxo_value = urllib2.unquote(taxo_value)
            taxo_value = self.convertNonAsciiToHTMLEntities(taxo_value)
            taxo_value = taxo_value.strip().replace(" ", "")
            par_taxo_md_dict = taxo_value_ar[0][2]
            taxo_md_dict = dict((k.lower(), v) for k,v in par_taxo_md_dict.iteritems())
            doc_lang = taxo_md_dict.get('model', 'ENGLISH')
            taxo_value = self.get_clean_number_by_lang(doc_lang_dict.get(doc_lang, 'G'), taxo_value)
            if '-' in taxo_value:
                taxo_value = taxo_value.replace('-', '')
                neg_flag = 1

            if '(' in taxo_value:
                taxo_value = taxo_value.replace('(', '')
                taxo_value = taxo_value.replace(')', '')
                neg_flag = 1
        
            #print neg_flag, "<br>"
            first_precision = 0
            if '.' in str(taxo_value):
                first_precision = len(str(taxo_value).split('.')[1])
            #print "first_precision====>", first_precision , "<br>"
            #print taxo_value
            taxo_value = self.__isfloat_int(taxo_value)
            #print "after float ====>", taxo_value , "<br>"
            taxo_value = taxo_value * int(doc_unit_dict.get(doc_unit, 1))
            #print "unit====>", int(doc_unit_dict.get(doc_unit, 1)), "<br>"
            #print "after mul ====>", taxo_value , "<br>"
            if '.' in str(taxo_value):
                final_precision = len(str(taxo_value).split('.')[1])
                #print "final_precision====>", final_precision , "<br>"
                if (first_precision > final_precision):
                    precision_value = '0'*(first_precision-final_precision)
                    #print "precision value ====>", precision_value, "<br>"
                    taxo_value = str(taxo_value) + str(precision_value)
                    #print "taxo value ====>", taxo_value, "<br>"
                else:
                    taxo_value = round(taxo_value, first_precision)
            
        else:
            #print "hi"
            #print 'taxo_value_ar: ', taxo_value_ar
            taxo_value = taxo_value_ar[0][0].strip().replace(" ", "")
            doc_unit = taxo_value_ar[0][1]
            par_taxo_md_dict = taxo_value_ar[0][2]
            taxo_md_dict = dict((k.lower(), v) for k,v in par_taxo_md_dict.iteritems())
            doc_lang = taxo_md_dict.get('model', 'ENGLISH')
            taxo_value = urllib2.unquote(taxo_value)
            taxo_value = self.convertNonAsciiToHTMLEntities(taxo_value)
            taxo_value = taxo_value.strip().replace(" ", "")
            #print "-----------------------------"
            #print "taxo_value_ar------>", taxo_value, "<br>"
            taxo_value = self.get_clean_number_by_lang(doc_lang_dict.get(doc_lang, 'G'), taxo_value)
            #print "taxo_value_ar------>", taxo_value, "<br>"
            #print doc_lang, "<br>"

            if '-' in taxo_value:
                taxo_value = taxo_value.replace('-', '')
                neg_flag = 1

            if '(' in taxo_value:
                taxo_value = taxo_value.replace('(', '')
                taxo_value = taxo_value.replace(')', '')
                neg_flag = 1

            taxo_value = self.__isfloat_int(taxo_value)
            taxo_value = taxo_value * int(doc_unit_dict.get(doc_unit, 1))
            if not taxo_value:
                taxo_value = 0
            #print taxo_value, '<br>'
            first_precision = 0
            if '.' in str(taxo_value):
                first_precision = len(str(taxo_value).split('.')[1])
            for cur_taxo_value in taxo_value_ar[1:]:
                each_taxo_value = str(cur_taxo_value[0]).strip().replace(' ', '')
                doc_unit = cur_taxo_value[1]
                par_taxo_md_dict = cur_taxo_value[2]
                taxo_md_dict = dict((k.lower(), v) for k,v in par_taxo_md_dict.iteritems())
                doc_lang = taxo_md_dict.get('model', 'ENGLISH')
                each_taxo_value = urllib2.unquote(each_taxo_value)
                each_taxo_value = self.convertNonAsciiToHTMLEntities(each_taxo_value)
                each_taxo_value = each_taxo_value.strip().replace(" ", "")
                each_taxo_value = self.get_clean_number_by_lang(doc_lang_dict.get(doc_lang, 'E'), each_taxo_value)
                if not each_taxo_value.strip():continue
                if '.' in str(each_taxo_value):
                    sec_precision = len(str(each_taxo_value).split('.')[1])
                    if first_precision < sec_precision:
                        first_precision = sec_precision
                neg_flag = 0
                if '-' in each_taxo_value:
                    each_taxo_value = each_taxo_value.replace('-', '')
                    neg_flag = 1

                if '(' in str(each_taxo_value):
                    each_taxo_value = each_taxo_value.replace('(', '')
                    each_taxo_value = each_taxo_value.replace(')', '')
                    neg_flag = 1

                each_taxo_value = self.__isfloat_int(each_taxo_value)
                if not each_taxo_value:continue
                #print 'each_taxo_value: ', each_taxo_value, '<br>'
                each_taxo_value = each_taxo_value * int(doc_unit_dict.get(doc_unit, 1))
                if neg_flag:
                    taxo_value -= each_taxo_value
                else:
                    #print "taxo_value====>", taxo_value
                    #print "each_taxo_value====>", [each_taxo_value]
                    #print type(taxo_value)
                    #print type(each_taxo_value)
                    taxo_value += each_taxo_value
            if '.' in str(taxo_value):
                final_precision = len(str(taxo_value).split('.')[1])
                #print 'final_precision: ', final_precision, first_precision, '<br>'
                if (first_precision > final_precision):
                    precision_value = 0*(first_precision-final_precision)
                    taxo_value = str(taxo_value) + str(precision_value)
                else:
                    taxo_value = round(taxo_value, first_precision)
        if taxo_value == '': return taxo_value
        if('-' not in str(taxo_value)) and ('--' not in str(taxo_value)) and taxo_value and (type(taxo_value) != str):
            locale.setlocale(locale.LC_ALL, 'en_US')
            #print "inside_negate", neg_flag, taxo_value, "<br>"
            taxo_value = locale.currency(taxo_value, symbol='', grouping=True)
            if neg_flag:
                #taxo_value = '(%s)' %(taxo_value)
                taxo_value = '-%s' %(taxo_value)

        #print 'after_clean:' ,taxo_value, '<br>'
        return taxo_value

    def get_clean_value_new(self, svalue):
        svalue = ''.join(svalue.strip().split())
        svalue = svalue.replace(';', '').replace('$ ', '').replace('$', '').replace('%', '').replace('&nbsp', '').replace('..', '.')
        if ('(' in svalue and ')' in svalue):
            ast = 'abcdefghijklmnopqrstuvwxyz'
            for e in ast:
                sb = '('+e+')'
                ss = '('+e.upper()+')'
                svalue = svalue.replace(ss, '')
                svalue = svalue.replace(sb, '')
        svalue = svalue.replace('&#162', '').replace('&#165', '').replace('&#65288', '(').replace('&#65289', ')').replace('&#65293', '').replace('&#12540', '').replace('&#163', '').replace('&#165', '')
        flip_sign = ''
        if ('(' in svalue and ')' in svalue) or (svalue.startswith('-') > 0):
            flip_sign = '-1'
        if '&#8208' in svalue:
            flip_sign = '-1'
        svalue = svalue.replace('&#8211', '').replace('\xe2\x80\x93', '').replace('&#8364', '').replace('&#8208', '').replace('&#8722', '').replace('&#8212', '').replace('&#160', '').replace('&#8213', '').replace('&#402', '')
        dot_count = 0
        new_string = ''
        for e in svalue:
            e = e.strip()
            if e in '0123456789':
               new_string += e
            elif e in '.':
               new_string += e 
               dot_count = dot_count + 1 
            else:
               continue     

        if (dot_count > 1): 
           return 0
        mul_factor = 1
        if flip_sign:
           mul_factor = -1     
        try:
             if (dot_count == 1):  
                new_num = mul_factor*float(new_string)
             else: 
                new_num = mul_factor*int(new_string)
        except:
             new_num = 0
        return new_num 

if __name__ == "__main__":

    obj = CleanCellValueByLang()
    taxo_value = "1.000.10,103"
    md_data = {"model":'German', 'units':'Bn'}
    #taxo_value = "1.000.10,103"
    #md_data = {"model":'german', 'units':'actual'}
    tv = obj.clean_cell_value(taxo_value, md_data)
    print str(tv)
   
