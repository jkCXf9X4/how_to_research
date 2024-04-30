
import re
import keyring
import json
from openai import OpenAI

from sequence_extensions import dict_ext



class ChatGPTWrapper:

    client = OpenAI(api_key=keyring.get_password("openai", "openai"))


    def __init__(self, text, temperature=0.0, model="gpt-3.5-turbo" ) -> None:
        self.temperature = temperature
        self.model = model
        self.input_text = text

        self.answer = dict_ext()
        
    def query(self):
        query = {}
        query["messages"] = [{"role": "system", "content": "Assistant is a large language model trained by OpenAI."},
                            { "role": "user", "content": self.input_text}]
                            #  { "role": "user", "content": "Say This is a test"}]
        query["temperature"] = self.temperature
        query["model"] = self.model

        chat_completion = self.client.chat.completions.create(**query)

        self.answer = dict_ext(chat_completion.to_dict())
        self.answer["query"] = query

        return self.answer
    
    def get_content(self):
         return self.answer["choices"][0]["message"]["content"]
    
    def add_info(self, **kwargs):
        self.answer = self.answer.extend(kwargs)
    
    def save_query(self, path):
        with open(path, "w") as f:
            json.dump(self.answer, f, indent=4)


