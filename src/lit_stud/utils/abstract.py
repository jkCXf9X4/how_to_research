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

        # should strip images
        # maybe use words that are lomger than 200 chars or similar

        return cleantext
    
    def __init__(self, text) -> None:
        self.original_text = text

        self.text = self.cleanhtml(text)

    def nr_of_words(self):
        return len(re.findall(r'\w+', self.text))
    
    def get_text(self):
        limit = 10000
        if len(self.text) > limit:
            print(f"[ERROR] abstract to large, cutting after {limit} letters")
            return self.text[:limit]
        else:
            return self.text

