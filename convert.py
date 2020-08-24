# -*- coding:utf-8 -*-
from BeautifulSoup import BeautifulStoneSoup
import cgi
import re
import htmlentitydefs
import urllib2

class convert(object):
    def __init__(self):
        pass

    def get_clean_number_by_lang(self, lang, txt):
        lang = lang.upper()
        txt = ''.join(map(lambda x:x.strip(), txt.split(' ')))
        txt = txt.replace('&#8364;', '')
        txt = txt.replace('&#8217;', "'")
        minus_list = ['&#8212;', '&#8213;', '&#8722;', '&#8211;', '&#8212;']
        for each in minus_list:
            txt = txt.replace(each, '-')
        txt = re.sub("[a-zA-Z$%*']+", '', txt)
        if lang in ['E', 'ENGLISH']:
            txt = txt.replace(',', '')
        if lang in ['G', 'GERMAN']:
           txt = txt.replace('.', '')
           txt = txt.replace(',', '.')
        txt = txt.strip()
        if '(' and ')' in str(txt):
            txt = txt.replace('(', '').replace(')', '')
            txt = '-%s' % str(txt)
        else:
            txt = str(txt)
        txt = txt.strip()
        if txt in ['-']: txt = '0'
        txt = txt.replace("'", "")
        try:
           new_txt = int(txt)
        except:
           try:
              new_txt = float(txt)
           except:
              txt = ''
        return txt

    def HTMLEntitiesToUnicode1(self, text):
        def fixup(m):
            text = m.group(0)
            if text[:2] == "&#":
                # character reference
                try:
                    if text[:3] == "&#x":
                        return unichr(int(text[3:-1], 16))
                    else:
                        return unichr(int(text[2:-1]))
                except ValueError:
                    print "Value Error"
                    pass
            else:
                # named entity
                # reescape the reserved characters.
                try:
                    if text[1:-1] == "amp":
                        text = "&"
                    elif text[1:-1] == "gt":
                        text = ">"
                    elif text[1:-1] == "lt":
                        text = "<"
                    else:
                        print text[1:-1]
                        text = unichr(htmlentitydefs.name2codepoint[text[1:-1]])
                except KeyError:
                    print "keyerror"
                    pass
            return text # leave as is
        return re.sub("&#?\w+;", fixup, text)

    def HTMLEntitiesToUnicode(self, text):
        """Converts HTML entities to unicode.  For example '&amp;' becomes '&'."""
        text = unicode(BeautifulStoneSoup(text, convertEntities=BeautifulStoneSoup.ALL_ENTITIES))
        return text

    def unicodeToHTMLEntities(self, text):
        """Converts unicode to HTML entities.  For example '&' becomes '&amp;'."""
        text = cgi.escape(text).encode('ascii', 'xmlcharrefreplace')
        return text

    def convertNonAsciiToHTMLEntities(self, text):
        if not type(text) in [str, unicode]:
            text = str(text)
        text = text.replace('&amp;', '&')
        try:
            text = text.decode('utf-8', errors='ignore')
        except:
            print 'data error :[%s]' %text
            text = ''
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
        new_txt = new_txt.replace('&#173;', '-')
        new_txt = new_txt.replace('%u2014', '-')
        new_txt = new_txt.replace('&#226;&#128;&#8221;', '-')
        new_txt = new_txt.replace('&#226;&#128;&#148;', '-')
        new_txt = new_txt.replace('&amp;', '&')
        new_txt = new_txt.replace('&#8212;', '-')
        new_txt = new_txt.replace('&#8220;', '"')
        new_txt = new_txt.replace('&#8217;', "'")
        new_txt = new_txt.replace('&#8221;', '"')
        new_txt = new_txt.replace('&#8211;', '-')
        cp1252_to_unicode_dict = {'&#92;':'&#2019;', '&#145;':'&#8216;', '&#146;':'&#8217;', '&#147;':'&#8220;', '&#148;':'&#8221;', '&#149;':'&#8226;', '&#150;':'&#8211;', '&#151;':'&#8212;', '&#152;':'&#732;', '&#153;':'&#8482;'}
        for k, v in cp1252_to_unicode_dict.items():
            new_txt = new_txt.replace(k, v)
            #try:
            # find_entities[k] = 1
        error_keys = []
        find_keys = find_entities.keys()
        for find_key in find_keys:
            flg = cp1252_to_unicode_dict.get(find_key, 0)
            if flg == 0:
               error_flg = 1
               error_keys.append(find_key)
               break
        if error_flg:
           pass
        new_txt = str(' '.join(new_txt.split()))
        return new_txt


if __name__ == '__main__':
    obj = convert()
    #text = "&amp;, &reg;, &lt;, &gt;, &cent;, &pound;, &yen;, &euro;, &sect;, &copy;"
    #uni = obj.HTMLEntitiesToUnicode1(text)
    #htmlent = obj.unicodeToHTMLEntities(uni)
    #txt = urllib2.unquote('%26%238212%3B')
    txt = 'a. \x01 test'
    txt = obj.convertNonAsciiToHTMLEntities(txt)
    print 'txt: ', [txt]
    #print uni
    #print htmlent
