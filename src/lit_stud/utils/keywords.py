
from sequence_extensions import list_ext

class KeywordGroup:

    def __init__(self, name, words, weight=1) -> None:
        self.name = name
        self.words = list_ext(words)
        self.weight :int = weight

    def __repr__(self) -> str:
        return f"{self.name}: [{self.weight}] - {' '.join(self.words)}"
    
    def contains(self, text):
        for i in self.words:
            if i in text:
                return self.weight
        return 0
            

class KeywordGroups(list):

    def evaluate_keywords(self, text):
        value = 0
        for key_group in self:
            key_group : KeywordGroup
            value += key_group.contains(text)
        return value
    
    def __repr__(self) -> str:
        s = ""
        for i in self:
            s += f"{i}\n"
        return s