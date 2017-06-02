#!/usr/bin/python 
from obo_parser import GODag
import re
from pymongo import MongoClient
import pymongo 
import datetime


client=MongoClient('mongodb://10.100.200.129:27017')
db=client.sanger_biodb
coll=db.GO
go_path = '/mnt/ilustre/users/sanger-dev/app/database/GO/go_new/go.obo'

ids = [] 
defins = []
synonyms = [] 
isa_list = []  
f=open(go_path).read()
termrecords=f.split('\n'+'\n')[1:] 
for item in termrecords:
    itemrecord=item.split('\n')
    record=[]
    for r in itemrecord:
        if r!='':
            record.append(r)  
    synonymcollect=[]
    isacollection = []
    null = []
    if record!=[]:  
        if record[0]=='[Term]':
            for keyvaluepair in record: 
                if keyvaluepair.startswith('id'):
                    theid=keyvaluepair[4:] 
                    ids.append(theid) 
                elif keyvaluepair.startswith("def:"):
                    thecount=[]
                    for j in range(0,len(keyvaluepair)):
                        if keyvaluepair[j]=='"':
                            thecount.append(j)
                    definition=keyvaluepair[thecount[0]+1:thecount[-1]]  
                    defins.append(definition)
                elif keyvaluepair.startswith("synonym:"):
                    scount=[]
                    for k in range(0,len(keyvaluepair)):
                        if keyvaluepair[k]=='"':
                            scount.append(k)
                    thesynonym=keyvaluepair[scount[0]+1:scount[-1]]  
                    synonymcollect.append(thesynonym)
                elif keyvaluepair.startswith('is_a'): 
                    isa=keyvaluepair.split()[1]  
                    isacollection.append(isa)                     
            if synonymcollect!=[]: 
                synonyms.append(synonymcollect)
            else:
                synonyms.append(null)
            if isacollection != []:
                isa_list.append(isacollection)
            else:
                isa_list.append(null) 

obodag = GODag(go_path) 
count = 1
for i in range(len(ids)): 
    id = ids[i]
    go_obj = obodag.get(id, None)  
    go_id = go_obj.id
    name = go_obj.name
    ontology = go_obj.namespace
    level_ = str(go_obj.level)
    depth_ = str(go_obj.depth)
    level = int(level_)
    depth = int(depth_)
    synonym = synonyms[i]
    defin = defins[i]  
    isa = isa_list[i]
    try:
        result=coll.insert_one(
            {'go_id':go_id, 'name':name, 'ontology':ontology, 'synonym':synonym, 'definition':defin, 
            'level':level, 'depth':depth, 'is_a': isa, 'version': float(1.0)}
        )
        count += 1
    except pymongo.errors.DuplicateKeyError:
        pass 
print "go:"
print count
