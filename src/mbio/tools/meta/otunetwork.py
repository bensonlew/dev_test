# -*- coding: utf-8 -*-
# __author__ = 'JieYap'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
import types
import subprocess
from biocluster.core.exceptions import OptionError


class OtunetworkAgent(Agent):
    """
    需要calc_otu_network.py
    version 1.0
    author: JieYao
    last_modified:2016.8.1
    """
    
    def __init__(self, parent):
        super(OtunetworkAgent, self).__init__(parent)
        options = [
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table, meta.otu.tax_summary_dir"},
            {"name": "level", "type": "string", "default": "otu"},
            {"name": "envtable", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "envlabs", "type": "string", "default": ""}
            ]
        self.add_option(options)
        self.step.add_steps('OtunetworkAnalysis')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.OtunetworkAnalysis.start()
        self.step.update()
        
    def step_end(self):
        self.step.OtunetworkAnalysis.finish()
        self.step.update()
        
    def gettable(self):
        """
        根据输入的otu表和分类水平计算新的otu表
        :return:
        """
        if self.option('otutable').format == "meta.otu.tax_summary_dir":
            return self.option('otutable').get_table(self.option('level'))
        else:
            return self.option('otutable').prop['path']
        
    def check_options(self):
        """
        重写参数检查
        """
        if not self.option('otutable').is_set:
            raise OptionError('必须提供otu表')
        self.option('otutable').get_info()
        if self.option('otutable').prop['sample_num'] < 2:
            raise OptionError('otu表的样本数目少于2，不可进行网络分析')
        if self.option('envtable').is_set:
            self.option('envtable').get_info()
            if self.option('envlabs'):
                labs = self.option('envlabs').split(',')
                for lab in labs:
                    if lab not in self.option('envtable').prop['group_scheme']:
                        raise OptionError('envlabs中有不在物种(环境因子)表中存在的因子：%s' % lab)
            else:
                pass
            if len(self.option('envtable').prop['sample']) < 2:
                raise OptionError('物种(环境因子)表的样本数目少于2，不可进行网络分析')
        samplelist = open(self.gettable()).readline().strip().split('\t')[1:]
        if self.option('envtable').is_set:
            self.option('envtable').get_info()
            if len(self.option('envtable').prop['sample']) > len(samplelist):
                raise OptionError('OTU表中的样本数量:%s少于物种(环境因子)表中的样本数量:%s' % (len(samplelist),
                                  len(self.option('envtable').prop['sample'])))
            for sample in self.option('envtable').prop['sample']:
                if sample not in samplelist:
                    raise OptionError('物种(环境因子)表的样本中存在OTU表中未知的样本%s' % sample)
        table = open(self.gettable())
        if len(table.readlines()) < 4 :
            raise OptionError('数据表信息少于3行')
        table.close()
        return True

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 2
        self._memory = ''
        
    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
                [".", "", "OTU网络分析结果输出目录"],
                ["./real_node_table.txt", "txt", "OTU网络节点属性表"],
                ["./real_edge_table.txt", "txt", "OTU网络边集属性表"],
                ["./real_dc_otu_degree.txt", "txt", "OTU网络OTU节点度分布表"],
                ["./real_dc_sample_degree.txt", "txt", "OTU网络sample节点度分布表"],
                ["./real_dc_sample_otu_degree.txt", "txt", "OTU网络节点度分布表"],
                ["./network_centrality.txt", "txt", "OTU网络中心系数表"],
                ["./network_attributes.txt", "txt", "OTU网络单值属性表"],
                ])
        print self.get_upload_files()
        super(OtunetworkAgent, self).end()


class OtunetworkTool(Tool):
    def __init__(self, config):
        super(OtunetworkTool, self).__init__(config)
        self._version = "1.0.1"
        self.cmd_path = self.config.SOFTWARE_DIR + '/bioinfo/meta/scripts/calc_otu_network.py'
        self.env_table = self.get_new_env()
        self.otu_table = self.get_otu_table()
        self.out_files = ['real_node_table.txt', 'real_edge_table.txt', 'real_dc_otu_degree.txt', 'real_dc_sample_degree.txt', 'real_dc_sample_otu_degree.txt', 'network_centrality.txt', 'network_attributes.txt']
        
        
    def get_otu_table(self):
        """
        根据调用的level参数重构otu表
        :return:
        """
        if self.option('otutable').format == "meta.otu.tax_summary_dir":
            otu_path = self.option('otutable').get_table(self.option('level'))
        else:
            otu_path = self.option('otutable').prop['path']
        if self.option('envtable').is_set:
            return self.filter_otu_sample(otu_path, self.option('envtable').prop['sample'],
                                          os.path.join(self.work_dir, 'temp_filter.otutable'))
        else:
            return otu_path
    
    def filter_otu_sample(self, otu_path, filter_samples, newfile):
        if not isinstance(filter_samples, types.ListType):
            raise Exception('过滤otu表样本的样本名称应为列表')
        try:
            with open(otu_path, 'rb') as f, open(newfile, 'wb') as w:
                one_line = f.readline()
                all_samples = one_line.rstrip().split('\t')[1:]
                if not ((set(all_samples) & set(filter_samples)) == set(filter_samples)):
                    raise Exception('提供的过滤样本集合中存在otu表中不存在的样本all:%s,filter_samples:%s' % (all_samples, filter_samples))
                if len(all_samples) == len(filter_samples):
                    return otu_path
                samples_index = [all_samples.index(i) + 1 for i in filter_samples]
                w.write('OTU\t' + '\t'.join(filter_samples) + '\n')
                for line in f:
                    all_values = line.rstrip().split('\t')
                    new_values = [all_values[0]] + [all_values[i] for i in samples_index]
                    w.write('\t'.join(new_values) + '\n')
                return newfile
        except IOError:
            raise Exception('无法打开OTU相关文件或者文件不存在')

    def get_new_env(self):
        """
        根据envlabs生成新的envtable
        """
        if self.option('envlabs'):
            new_path = self.work_dir + '/temp_env_table.xls'
            self.option('envtable').sub_group(new_path, self.option('envlabs').split(','))
            return new_path
        else:
            return self.option('envtable').path

    def run(self):
        """
        运行
        """
        super(OtunetworkTool, self).run()
        self.run_otu_network_py()

    def formattable(self, tablepath):
        alllines = open(tablepath).readlines()
        if alllines[0][0] == '#':
            newtable = open(os.path.join(self.work_dir, 'temp_format.table'), 'w')
            newtable.write(alllines[0].lstrip('#'))
            newtable.writelines(alllines[1:])
            newtable.close()
            return os.path.join(self.work_dir, 'temp_format.table')
        else:
            return tablepath

    def run_otu_network_py(self):
        """
        运行calc_otu_network.py
        """
        real_otu_path = self.formattable(self.otu_table)
        cmd = self.config.SOFTWARE_DIR + '/program/Python/bin/python '
        cmd += self.cmd_path
        cmd += ' -i %s -o %s' % (real_otu_path, self.work_dir + '/otu_network')
        if self.option('envtable').is_set:
            cmd += ' -m %s' % (self.env_table)
        self.logger.info('开始运行calc_otu_network生成OTU网络并进行计算')
        
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info('OTU_Network计算完成')
        except subprocess.CalledProcessError:
            self.logger.info('OTU_Network计算失败')
            self.set_error('运行calc_otu_network.py失败')
        allfiles = self.get_filesname()
        for i in range(len(self.out_files)):
            self.linkfile(allfiles[i], self.out_files[i])
        self.end()

    def linkfile(self, oldfile, newname):
        """
        link文件到output文件夹
        :param oldfile: 资源文件路径
        :param newname: 新的文件名
        :return:
        """
        newpath = os.path.join(self.output_dir, newname)
        if os.path.exists(newpath):
            os.remove(newpath)
        os.link(oldfile, newpath)

    def get_filesname(self):
        files_status = [None, None, None, None, None, None, None]
        for paths,d,filelist in os.walk(self.work_dir + '/otu_network'):
            for filename in filelist:
                name = os.path.join(paths, filename)
                for i in range(len(self.out_files)):
                    if self.out_files[i] in name:
                        files_status[i] = name
        for i in range(len(self.out_files)):
            if not files_status[i]:
                self.set_error('未知原因，结果文件生成出错或丢失')
        return files_status
