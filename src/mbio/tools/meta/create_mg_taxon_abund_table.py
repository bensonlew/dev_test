# -*- coding: utf-8 -*-
# __author__ = 'gaohao'
# last_modifiy:2017.10.18

from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
import pandas as pd


class CreateMgTaxonAbundTableAgent(Agent):
    """
    生成物种注释的丰度表
    """
    def __init__(self, parent):
        super(CreateMgTaxonAbundTableAgent, self).__init__(parent)
        options = [
            {"name": "anno_table", "type": "infile", "format": "meta.profile"},  # 各数据库的注释表格
            {"name": "geneset_table", "type": "infile", "format": "meta.otu.otu_table"},
            {"name": "out_table", "type": "outfile", "format": "meta.otu.otu_table"},
        ]
        self.add_option(options)

    def check_options(self):
        if not self.option("anno_table").is_set :
            raise OptionError("请传入物种注释表格文件！")
        if not self.option("geneset_table").is_set:
            raise OptionError("请传入基因丰度文件！")


    def set_resource(self):
        self._cpu = 1
        self._memory = '2G'

    def end(self):
        super(CreateMgTaxonAbundTableAgent, self).end()


class CreateMgTaxonAbundTableTool(Tool):
    def __init__(self, config):
        super(CreateMgTaxonAbundTableTool, self).__init__(config)

    def create_abund_table(self):
        geneset_table_path = self.option("geneset_table").prop["path"]
        geneset_table = pd.read_table(geneset_table_path, sep='\t', header=0)
        otu_file_path = os.path.join(self.output_dir, "abund_table.xls")
        new_otu_file_path = os.path.join(self.output_dir, "new_abund_table.xls")
        anno_table_path = self.option("anno_table").prop["path"]
        anno_table = pd.read_table(anno_table_path, sep='\t', header=0)
        a = pd.DataFrame(anno_table)
        a['Taxonomy']=a['Domain']+";"+a['Kingdom']+";"+a['Phylum']+";"+a['Class']+";"+a['Order']+";"+a['Family']+";"+a['Genus']+";"+a['Species']
        a=a.ix[:,["#Query","Taxonomy"]]
        a.columns = ["GeneID","Taxonomy"]
        b = pd.DataFrame(geneset_table)
        del b["Total"]
        abund = b.merge(a, on='GeneID', how='inner')
        Last_abund=abund.drop(['GeneID'],axis=1)
        level_type_abund_table=Last_abund.groupby("Taxonomy").sum()               
        level_type_abund_table.to_csv(otu_file_path,sep='\t')
        self.last_abund_table(otu_file_path,new_otu_file_path)
    
    def last_abund_table(self,input,output):
        file = open(output,'w')
        with open(input,'r') as f:
                    lines = f.readlines()
                    name = lines[0]
                    names=name.strip().split('\t')
                    del names[0]
                    names.append('taxonomy')
                    new_name='\t'.join(names)
                    last_name="Taxon ID"+"\t"+new_name
                    file.write(last_name+'\n')
                    num =0  
                    for line in lines[1:]:
                        num +=1 
                        line = line.strip().split('\t')
                        line.append(line[0])
                        line[0]='tax'+str(num)
                        line='\t'.join(line)
                        file.write(line+'\n')
                    file.close()

    def set_output(self):
        self.logger.info('开始设置输出结果文件')
        try:
            self.option('out_table', os.path.join(self.output_dir, "new_abund_table.xls"))
            self.logger.info(self.option('out_table').prop['path'])
        except Exception as e:
            raise Exception("输出结果文件异常——{}".format(e))

    def run(self):
        super(CreateMgTaxonAbundTableTool, self).run()
        self.create_abund_table()
        self.set_output()
        self.end()
