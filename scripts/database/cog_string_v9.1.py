from pymongo import MongoClient
import re
import datetime


client = MongoClient('mongodb://10.100.200.129:27017')
db = client.sanger_biodb
coll = db.COG_String_V9

string_path = '/mnt/ilustre/users/sanger-dev/sg-users/zengjing/database/string_cog/cog_mapping_v9.txt'
f = open(string_path).read().split('\n')
for item in f:
    item = item.split('\t')
    string_id = item[0]
    start_position = item[1]
    end_position = item[2]
    orthologous_group = item[3]
    protein_annotation = item[4]
    function_categories = item[5]
    line = string_id + '\t' + start_position + '\t' + end_position + '\t' + orthologous_group + '\t' + protein_annotation + '\t' + function_categories
    result = coll.insert_one({
        'string_id': string_id,
        'start_position': start_position,
        'end_position': end_position,
        'orthologous_group': orthologous_group,
        'protein_annotation': protein_annotation,
        'function_categories': function_categories,
        'version': float(9.1)
    })
