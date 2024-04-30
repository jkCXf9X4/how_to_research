
import json
import datetime
from sequence_extensions import dict_ext

class ConfigLogUtils:

    @staticmethod
    def log_config(path, data : dict):
        
        d = dict_ext(data)
        d = d.map_dict(lambda key, value: (key, str(value)))

        t = datetime.datetime.now()
        d["time"] = str(t)
        # print(d)
        with open(path, "w") as file:
            json.dump(d, file)