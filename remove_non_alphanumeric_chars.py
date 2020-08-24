import re
def remove_non_alphanumeric_chars(input_string): # input: string (ascii), output: unicode object
     input_string = re.sub('[^0-9a-zA-Z]+', '', input_string)
     return input_string
