# -*- coding: utf-8 -*-
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError
from mbio.files.meta.otu.otu_table import OtuTableFile
import pandas as pd

class FactorDistanceAgent(Agent):
    """
    calculate distance of factor matrix according to given matrix type
    author: wangbixuan
    last modified: 20160712
    """
    MATRIXFACTOR=['abund_jaccard', 'binary_chisq', 'binary_chord', 'binary_euclidean',
    'binary_hamming', 'binary_jaccard', 'binary_lennon', 'binary_ochiai',
    'binary_otu_gain', 'binary_pearson', 'binary_sorensen_dice', 'bray_curtis',
    'bray_curtis_faith', 'bray_curtis_magurran', 'canberra', 'chisq', 'chord',
    'euclidean', 'gower', 'hellinger', 'kulczynski', 'manhattan', 'morisita_horn',
    'pearson', 'soergel', 'spearman_approx', 'specprof']

    def __init__(self,parent):
        super(FactorDistanceAgent,self).__init__(parent)
        options = [
            {"name":"factor","type":"infile","format":"meta.otu.group_table"},
            {"name":"facmatrixtype","type":"string","default":"bray_curtis"},
            {"name":"factorselected","type":"string","default":""},
            {"name":"fac_matrix","type":"outfile","format":"meta.beta_diversity.distance_matrix"}
        ]
        self.add_option(options)
        self.step.add_steps('factor_distance')
        self.on('start',self.step_start)
        self.on('end',self.step_end)

    def step_start(self):
        self.step.factor_distance.start()
        self.step.update()

    def step_end(self):
        self.step.factor_distance.finish()
        self.step.update()

    def check_options(self):
        if not self.option('factor').is_set:
            raise OptionError('Factor table not provided')
        else:
            self.option('factor').get_info()
            if self.option('factorselected'):
                factors=self.option('factorselected').split(',')
                for f in factors:
                    if f not in self.option('factor').prop['group_scheme']:
                        raise OptionError('such factor not included from original factor table：%s' %f)
            else:
                pass
        if self.option('facmatrixtype') not in FactorDistanceAgent.MATRIXFACTOR:
            raise OptionError('Selected matrix type is not supported.')

    def set_resource(self):
        self._cpu = 5
        self._memory = '5G'

    def end(self):
        result_dir=self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".","","环境因子距离矩阵结果输出目录"],
            ])
        result_dir.add_regexp_rules([
            [r'%s.*\.xls' % self.option('facmatrixtype'),'xls','环境因子距离矩阵文件']
            ])
        super(FactorDistanceAgent,self).end()

class FactorDistanceTool(Tool):
    def __init__(self,config):
        super(FactorDistanceTool, self).__init__(config)
        self._version = '1.9.1'  # qiime版本
        self.cmd_path = 'program/Python/bin/beta_diversity.py'
        # 设置运行环境变量
        self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + 'gcc/5.1.0/lib64:$LD_LIBRARY_PATH')
        self.biom = self.biom_fac_table()  # 传入otu表需要转化为biom格式

    def run(self):
        super(FactorDistanceTool,self).run()
        self.run_beta_diversity()

    def run_beta_diversity(self):
        cmd=self.cmd_path
        cmd+=' -m %s -i %s -o %s'%(self.option('facmatrixtype'),self.biom,self.work_dir)
        self.logger.info('run beta_diversity.py program')
        fac_matrix_command=self.add_command('fac_matrix',cmd)
        fac_matrix_command.run()
        self.wait()
        if fac_matrix_command.return_code==0:
            self.logger.info('Succeed on calculating factor matrix')
            filename=self.work_dir + '/' + \
                self.option('facmatrixtype') + '_temp.txt'
            linkfile=self.output_dir+'/factor_out'+'.xls'
            if os.path.exists(linkfile):
                os.remove(linkfile)
            os.link(filename,linkfile)
            self.option('fac_matrix', linkfile)
            self.end()
        else:
            self.set_error('Error in running beta_diversity.py')


    def biom_fac_table(self):
        if self.option('factorselected'):
            #program to extract selected information begins
            with open('extractfactor.txt','w') as f:
                orifile=open(self.option('factor').prop['path']).read().split('\n')
                selectedf=self.option('factorselected').split(',')
                storeposi=[]
                allf=orifile[0].split('\t') #include #name
                for i in range(0,len(allf)):
                    if i==0:
                        f.write(allf[i]+'\t')
                    else:
                        if allf[i] in selectedf:
                            storeposi.append(i)
                            if len(storeposi)<len(selectedf):
                                f.write(allf[i]+'\t')
                            else:
                                f.write(allf[i])
                f.write('\n')
                for record in orifile[1:]:
                    item=record.split('\t')
                    for j in range(0,len(item)):
                        if j==0:
                            f.write(item[j]+'\t')
                        else:
                            if j in storeposi:
                                if j!=storeposi[-1]:
                                    f.write(item[j]+'\t')
                                else:
                                    if record!=orifile[-1]:
                                        f.write(item[j]+'\n')
                                    else:
                                        f.write(item[j])
            #program ends
            newtable='extractfactor.txt'
        else:
            newtable=self.option('factor').prop['path']
        #trans newtable column and index begins
        df=pd.read_csv(open(newtable),delim_whitespace=True)
        df0=pd.DataFrame()
        titles=df.columns
        samples=df.index
        for item in titles:
            for count in samples:
                df0.loc[item,count]=df.loc[count,item]
        newindex=df0.index
        newcolumns=df0.columns
        with open('transtable.txt','w') as tf:
            for title in newindex:
                if title==newindex[0]:
                    tf.write('ID'+'\t')
                else:
                    tf.write(title+'\t')
                for facname in newcolumns:
                    if facname!=newcolumns[-1]:
                        try:
                            tf.write(df0.loc[title,facname]+'\t')
                        except TypeError:
                            tf.write(str("%.4f"%df0.loc[title,facname])+'\t')
                    else:
                        try:
                            tf.write(df0.loc[title,facname])
                        except TypeError:
                            tf.write(str("%.4f"%df0.loc[title,facname]))
                if title!=newindex[-1]:
                    tf.write('\n')
        #trans ends
        #trans_newtable='transtable.txt'
        trans_newtable=OtuTableFile()
        trans_newtable.set_path('transtable.txt')
        biom_path = os.path.join(self.work_dir, 'temp.biom')
        if os.path.isfile(biom_path):
            os.remove(biom_path)
        #newtable.convert_to_biom(biom_path)
        trans_newtable.check()
        trans_newtable.convert_to_biom(biom_path)
        return biom_path
