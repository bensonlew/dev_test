from pymongo import MongoClient
import datetime


client = MongoClient('mongodb://10.100.200.129:27017')
db = client.sanger_biodb
coll = db.COG_V9
des = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/denovo_rna/daobiao/reference_database/NOG_description_v9.1.txt"
nog = "/mnt/ilustre/users/sanger-dev/sg-users/zengjing/denovo_rna/daobiao/reference_database/COG_function_v9.1.txt"
f2 = open(des).read().split('\n')
f1 = open(nog).readlines()
f3 = open('eggnogv4.funccats.txt').read().split('\n\n')
for item1 in f1[1:]:
    if item1 != '':
        item1 = item1.strip().split('\t')
        cog_id = item1[0]
        cog_categ = ','.join(item1[1].split())
        for item2 in f2:
            item2 = item2.split('\t')
            if cog_id == item2[0]:
                cog_desc = item2[1]
                for item3 in f3:
                    item3 = item3.split('\n')
                    categ_type = item3[0]
                    keys = item3[1:]
                    for item in keys:
                        if cog_categ == item[2]:
                            categ_descr = item[5:]
                            result = coll.insert_one(
                                {'cog_id': cog_id, 'cog_description': cog_desc, 'cog_categories': cog_categ,
                                 'categories_type': categ_type, 'categories_description': categ_descr, 'version': float(9.1)}
                            )
