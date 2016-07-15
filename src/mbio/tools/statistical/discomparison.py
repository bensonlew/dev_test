# -*- coding: utf-8 -*-
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import subprocess
import os

class DiscomparisonAgent(Agent):
    """
    discomparison:用于检验群落距离矩阵和环境变量距离矩阵之间的相关性
    version: 1.0
    author: wangbixuan
    last_modified: 20160711
    """
    MATRIX=['abund_jaccard', 'binary_chisq', 'binary_chord', 'binary_euclidean', 
    'binary_hamming', 'binary_jaccard', 'binary_lennon', 'binary_ochiai', 
    'binary_otu_gain', 'binary_pearson', 'binary_sorensen_dice', 'bray_curtis', 
    'bray_curtis_faith', 'bray_curtis_magurran', 'canberra', 'chisq', 'chord', 
    'euclidean', 'gower', 'hellinger', 'kulczynski', 'manhattan', 'morisita_horn', 
    'pearson', 'soergel', 'spearman_approx', 'specprof', 'unifrac', 'unweighted_unifrac', 
    'weighted_normalized_unifrac', 'weighted_unifrac'
    ]
    MATRIXFACTOR=['abund_jaccard', 'binary_chisq', 'binary_chord', 'binary_euclidean', 
    'binary_hamming', 'binary_jaccard', 'binary_lennon', 'binary_ochiai', 
    'binary_otu_gain', 'binary_pearson', 'binary_sorensen_dice', 'bray_curtis', 
    'bray_curtis_faith', 'bray_curtis_magurran', 'canberra', 'chisq', 'chord', 
    'euclidean', 'gower', 'hellinger', 'kulczynski', 'manhattan', 'morisita_horn', 
    'pearson', 'soergel', 'spearman_approx', 'specprof']

    def __init__(self,parent):
        super(DiscomparisonAgent,self).__init__(parent)
        options = [
            #{"name": "level", "type": "string", "default": "otu"},
            #{"name":"otutable","type":"infile","format":"meta.otu.otu_table, meta.otu.tax_summary_dir"},
            #{"name":"otumatrixtype","type":"string","default":"weighted_unifrac"},
            #{"name":"factor","type":"infile","format":"meta.otu.group_table"},
            #{"name":"factormatrixtype","type":"string","default":"bray_curtis"},
            #{"name":"factorselected","type":"string","default":""},
            #{"name":"newicktree","type":"infile","format":"meta.beta_diversity.newick_tree"},
            {"name":"partialmatrix","type":"infile","format":"meta.beta_diversity.distance_matrix"}, #not sure
            {'name':'otudistance','type':'infile','format':'meta.beta_diversity.distance_matrix'},
            {'name':'facdistance','type':'infile','format':'meta.beta_diversity.distance_matrix'}
        ]
        self.add_option(options)
        self.step.add_steps('mantel_test')
        self.on('start',self.step_start)
        self.on('end',self.step_end)

    def step_start(self):
        self.step.mantel_test.start()
        self.step.update()

    def step_end(self):
        self.step.mantel_test.finish()
        self.step.update()

    def gettable(self):
        """
        get matrix for calculation by level provided
        """
        if self.option("otutable").format=="meta.otu.tax_summary_dir":
            return self.option("otutable").get_table(self.option('level'))
        else:
            return self.option('otutable').prop['path']

    def check_options(self):
        '''
        if self.option("level") not in ['otu', 'domain', 'kindom', 'phylum', 'class', 'order',
                                        'family', 'genus', 'species']:
            raise OptionError("分类水平不正确")
        if not self.option("otutable").is_set:
            raise OptionError('必须提供otu表')
        self.option('otutable').get_info()
        if not self.option("factor").is_set:
            raise OptionError('必须提供环境因子表')
        else:
            self.option('factor').get_info()
            if self.option('factorselected'):
                factors=self.option('factorselected').split(',')
                for f in factors:
                    if f not in self.option('factor').prop['group_scheme']:
                        raise OptionError('该因子不存在于环境因子表：%s' %f)
            else:
                pass
        
        if self.option("otumatrixtype") not in DiscomparisonAgent.MATRIX:
            raise OptionError('otu距离矩阵计算方法不正确')
        if self.option("factormatrixtype") not in DiscomparisonAgent.MATRIXFACTOR:
            raise OptionError('环境因子距离矩阵计算方法不正确')
        '''
        if not self.option('otudistance').is_set:
            raise OptionError('必须提供otu距离表')
        if not self.option('facdistance').is_set:
            raise OptionError('必须提供环境因子距离表')
        '''
        if not self.option("newicktree").is_set: #not sure
            raise OptionError("必须提供newicktree")
        self.option("newicktree").get_info()
        #partial?
        '''

    def set_resource(self):
        self._cpu=5
        self._memory=''

    def end(self):
        result_dir=self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".","","Discomparison计算结果输出目录"],
            ["./mantel_results.txt","txt","Discomparison结果"]
            ])
        super(DiscomparisonAgent,self).end()

class DiscomparisonTool(Tool):
    def __init__(self,config):
        super(DiscomparisonTool,self).__init__(config)
        self.version='1.9.1' #qiime version?
        self.cmd_path='Python/bin/compare_distance_matrices.py'
        self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + 'gcc/5.1.0/lib64:$LD_LIBRARY_PATH')
        #self.real_otu=self.get_otu_table()
        #self.real_fac=self.get_factor_table()
        #self.real_tree=self.get_newick()

    '''
    def get_factor_table(self):
        fpath=self.option('factor').prop['path']
        return fpath

    def get_newick(self):
        tpath=self.option('newicktree').prop['path']
        return tpath


    def get_otu_table(self):
        """
        根据level返回进行计算的otu表路径
        :return:
        """
        if self.option('otutable').format == "meta.otu.tax_summary_dir":
            otu_path = self.option('otutable').get_table(self.option('level'))
        else:
            otu_path = self.option('otutable').prop['path']
        return otu_path
    '''

    def run(self):
        """
        运行
        """
        super(DiscomparisonTool,self).run()
        self.run_discomparison()
        #self.set_output()
        self.end()

    
    def run_discomparison(self):
        """
        run Mantel_test.pl
        """
        cmd=self.cmd_path
        #cmd+=" -otu '%s' -motu '%s' -factor '%s' -mfactor '%s' -t '%s'"%(self.real_otu,self.option('otumatrixtype'),self.real_fac,self.option('factormatrixtype'),self.real_tree)
        '''
        cmd+=" -otu %s -motu %s -factor %s -mfactor %s -t %s"%(self.real_otu,self.option('otumatrixtype'),self.real_fac,self.option('factormatrixtype'),self.real_tree)
        if self.option('factorselected'):
            cmd+=' -select_factor \"%s\"'%self.option(factorselected)
        '''
        '''
        cmd+=' -i %s,%s -o %s -n 999'%(self.option('otudistance').prop['path'],self.option('facdistance').prop['path'],self.work_dir)
        if self.option('partialmatrix').is_set:
            cmd+=" -c %s --method partial_mantel"%self.option('patialmatrix').prop['path']
        else:
            cmd+=" --method mantel"
        '''
        if self.option('partialmatrix').is_set:
            cmd+=' -i %s,%s -c %s -o %s --method partial_mantel -n 999'%(self.option('otudistance').prop['path'],self.option('partialmatrix').prop['path'],self.option('facdistance').prop['path'],self.work_dir)
        else:
            cmd+=' -i %s,%s -o %s --method mantel -n 999'%(self.option('otudistance').prop['path'],self.option('facdistance').prop['path'],self.work_dir)
        self.logger.info('运行compare_distance_matrices.py 判断相关性')
        self.logger.info(cmd)
        discomparison_command=self.add_command('distance_comparision',cmd)
        discomparison_command.run()
        self.wait()
        if discomparison_command.return_code==0:
            self.logger.info('running compare_distance_matrices.py succeed')
            if self.option('partialmatrix').is_set:
                filename=self.work_dir+'/partial_mantel_results.txt'
                linkfile=self.output_dir+'/partial_mantel_results.txt'
            else:
                filename=self.work_dir+'/mantel_results.txt'
                linkfile=self.output_dir+'/mantel_results.txt'
            if os.path.exists(linkfile):
                os.remove(linkfile)
            os.link(filename,linkfile)
        else:
            self.set_error('Error in running compare_distance_matrices.py')
                
    '''
    def set_output(self):
        if self.option('partialmatrix').is_set:
            os.link(self.work_dir + "/partial_mantel_results.txt",self.output_dir + "/partial_mantel_results.txt")
        else:
            os.link(self.work_dir + "/mantel_results.txt",self.output_dir + "/mantel_results.txt")
    '''