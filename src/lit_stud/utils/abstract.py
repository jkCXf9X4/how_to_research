from io import StringIO
from html.parser import HTMLParser
import re

class Abstract:

    html_tag = re.compile('<.*?>') 
    broken_tag = re.compile('&[a-zA-Z0-9;&/#]*;')
    spaces = re.compile('[ ]+') 
    newlines = re.compile('[ ]*[\n]+[ ]*') 
    tabs = re.compile('[\t]+') 

    @staticmethod
    def cleanhtml(raw_html):
        cleantext = re.sub(Abstract.html_tag, '', raw_html)
        cleantext = re.sub(Abstract.broken_tag, '', cleantext)

        cleantext = re.sub(Abstract.tabs, ' ', cleantext)
        cleantext = re.sub(Abstract.newlines, '\n', cleantext)
        cleantext = re.sub(Abstract.spaces, ' ', cleantext)

        # cleantext = cleantext.replace("\t", " ")
        # cleantext = cleantext.replace("  ", " ")
        # cleantext = cleantext.replace("\n\n", "\n")
        return cleantext
    
    def __init__(self, text) -> None:
        self.original_text = text

        self.text = self.cleanhtml(text)

    def nr_of_words(self):
        return len(re.findall(r'\w+', self.text))
    
    def get_text(self):
        return self.text


# class MLStripper(HTMLParser):
#     def __init__(self):
#         super().__init__()
#         self.reset()
#         self.strict = False
#         self.convert_charrefs= True
#         self.text = StringIO()

#     def handle_data(self, d):
#         self.text.write(d)

#     def get_data(self):
#         return self.text.getvalue()

# def strip_tags(html):

#     return cleanhtml(html)
#     # s = MLStripper()
#     # s.feed(html)
#     # return s.get_data()

