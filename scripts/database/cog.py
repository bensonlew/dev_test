from pymongo import MongoClient
import datetime

client = MongoClient('mongodb://10.100.200.129:27017')
db = client.sanger_biodb
coll = db.COG


f1 = open('/mnt/ilustre/users/sanger-dev/sg-users/zengjing/database/string_cog/NOG.funccat.txt').read().split('\n')
f2 = open('/mnt/ilustre/users/sanger-dev/sg-users/zengjing/database/string_cog/NOG.description.txt').read().split('\n')
f3 = open('/mnt/ilustre/users/sanger-dev/sg-users/zengjing/database/string_cog/eggnogv4.funccats.txt').read().split('\n\n')
for item1 in f1:
    if item1 != '':
        item1 = item1.split('\t')
        cog_id = item1[0]
        cog_categ = ','.join(item1[1].split())
        for item2 in f2:
            item2 = item2.split('\t')
            cog_desc = item2[1]
            if cog_id == item2[0]:
                for item3 in f3:
                    item3 = item3.split('\n')
                    categ_type = item3[0]
                    keys = item3[1:]
                    for item in keys:
                        if cog_categ == item[2]:
                            categ_descr = item[5:]
                            result = coll.insert_one(
                                {'cog_id': cog_id, 'cog_description': cog_desc, 'cog_categories': cog_categ,
                                 'categories_type': categ_type, 'categories_description': categ_descr, 'version': float(10.0)}
                            )
