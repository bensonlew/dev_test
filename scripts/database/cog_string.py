from pymongo import MongoClient
import re
import datetime


client = MongoClient('mongodb://10.100.200.129:27017')
db = client.sanger_biodb
coll = db.COG_String

string_path = '/mnt/ilustre/users/sanger-dev/sg-users/zengjing/database/string_cog/cog_mapping_v10.txt'
f = open(string_path).read().split('\n')
for item in f:
    item = item.split('\t')
    m1 = re.match(r'"(.*)"', item[0])
    m2 = re.match(r'"(.*)"', item[1])
    m3 = re.match(r'"(.*)"', item[2])
    m4 = re.match(r'"(.*)"', item[3])
    m5 = re.match(r'"(.*)"', item[4])
    m6 = re.match(r'"(.*)"', item[5])
    string_id = m1.group(1)
    start_position = m2.group(1)
    end_position = m3.group(1)
    orthologous_group = m4.group(1)
    protein_annotation = m5.group(1)
    function_categories = m6.group(1)
    result = coll.insert_one({
        'string_id': string_id,
        'start_position': start_position,
        'end_position': end_position,
        'orthologous_group': orthologous_group,
        'protein_annotation': protein_annotation,
        'function_categories': function_categories,
        'version': float(10.0)}
    )
