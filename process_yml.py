import pandas as pd
import yaml
import os
import csv

root = '/home/tprasad/villagemd/development/python/productionRepo/i2py/config/anonymizer/athena_health/'
counter = 0
df_list = []
list_of_columns = {}
list_of_anonymized_columns = {}
freq_of_anonymization_types = {}
for fn in os.listdir(root):
    with open(os.path.join(root, fn), 'r') as f:
        # print(f.name)
        df = pd.io.json.json_normalize(yaml.load(f))
        src_fields = df["source.fields"][0]
        for col in src_fields:
            if col["name"] in list_of_columns:
                list_of_columns[col["name"]] += 1
            else:
                list_of_columns[col["name"]] = 1
            if "anonymize" in col:
                if col["name"] in list_of_anonymized_columns:
                    list_of_anonymized_columns[col["name"]] += 1
                else:
                    list_of_anonymized_columns[col["name"]] = 1
                if col["anonymize"] in freq_of_anonymization_types:
                    freq_of_anonymization_types[col["anonymize"]] += 1
                else:
                    freq_of_anonymization_types[col["anonymize"]] = 1
        df_list.append(df)
