import re
def convert_htmlnumbers_to_char(input_string): # input: string (ascii), output: unicode object
      input_string = str(input_string)
      input_string = input_string.replace('&#8364;', '') ##Added By Anamika to remove euro sign
      matches = re.findall("&#\d+;", input_string)
      if len(matches) > 0:
         hits = set(matches)
         for hit in hits:
            name = hit[2:-1]
            try:
                entnum = int(name)
                input_string = input_string.replace(hit, unichr(entnum))
            except ValueError:
                pass
      return input_string

