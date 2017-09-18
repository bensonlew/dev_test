# -*- coding: utf-8 -*-
# __author__ = 'guhaidong'

"""宏基因组分析工作流"""

from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError
from bson import ObjectId
import os
import json
import shutil
import time
import functools


def time_count(func):  # 统计导表时间
    @functools.wraps(func)
    def wrapper(*args, **kw):
        start = time.time()
        func(*args, **kw)
        end = time.time()
        print("{}函数执行完毕，该阶段导表已进行{}s".format(func.__name__, end - start))
        return wrapper


class MetaGenomicWorkflow(Workflow):
    def __init__(self, wsheet_object):
        """
        宏基因组workflow option参数设置
        """
        self._sheet = wsheet_object
        super(MetaGenomicWorkflow, self).__init__(wsheet_object)
        options = [
            {'name': 'test', 'type': 'bool', 'default': False},  # 是否为测试workflow
            {'name': 'main_id', 'type': 'string'},  # 原始序列主表_id
            {'name': 'in_fastq', 'type': 'infile', 'format': 'sequence.fastq_dir'},  # 输入的fq文件或fq文件夹
            # {'name': 'fq_type', 'type': 'string', 'default': 'PE'},  # PE OR SE
            {'name': 'qc_quality', 'type': 'int', 'default': 20},  # 质控质量值标准
            {'name': 'qc_length', 'type': 'int', 'default': 30},  # 质控最短序列长度
            {'name': 'rm_host', 'type': 'bool', 'default': False},  # 是否需要去除宿主
            {'name': 'ref_database', 'type': 'string', 'default': ''},  # 宿主参考序列库中对应的物种名，eg：E.coli ,B.taurus
            {'name': 'ref_undefined', "type": 'infile', 'format': 'sequence.fasta_dir'},
            # 未定义的宿主序列所在文件夹，多个宿主cat到一个文件，并作为tool:align.bwa的输入文件，可不提供
            {'name': 'assemble_tool', 'type': 'string', 'default': 'idba'},  # 选择拼接工具，soapdenovo OR idba
            {'name': 'assemble_type', 'type': 'string', 'default': 'simple'},  # 选择拼接策略，simple OR multiple
            {'name': 'min_contig', 'type': 'int', 'default': 300},  # 拼接序列最短长度
            {'name': 'min_gene', 'type': 'int', 'default': 100},  # 预测基因最短长度
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        '''获取数据库信息'''
        self.json_path = self.config.SOFTWARE_DIR + "/database/Genome_DB_finish/annot_species.json"
        self.json_dict = self.get_json()
        '''初始化module/tool'''
        # self.qc = self.add_module('')
        self.rm_host = self.add_module('meta.qc.bwa_remove_host')
        self.assem_soapdenovo = self.add_module('assemble.mg_ass_soapdenovo')
        self.assem_idba = self.add_module('assemble.mg_ass_idba')
        self.gene_predict = self.add_module('gene_structure.gene_predict')
        # self.XXX = self.add_module("XXX")
        # self.XXX = self.add_tool("XXX")
        '''add_steps'''
        self.step.add_steps('qc_', 'rm_host', 'assem', 'gene_predict')
        '''初始化自定义变量'''
        if self.option('test'):
            self.option('main_id', '111111111111111111111111')
            self.qc_fastq = self.option('in_fastq')  #暂未加入质控步骤，输入质控序列

    def check_options(self):
        """
        检查参数设置
        """
        if not self.option('main_id') and self.option('test') == False:
            raise OptionError('缺少主表id')
        if not self.option('in_fastq'):
            raise OptionError('需要输入原始fastq序列')
        # if not self.option('fq_type') in ['PE', 'SE']:
        #    raise OptionError('fq序列应为PE或SE')
        if not self.option('qc_quality') > 0 and not self.option('qc_quality') < 42:
            raise OptionError('qc最小质量值超出范围，应在0~42之间')
        if not self.option('qc_length') > 0:
            raise OptionError('qc最小长度值超出范围，应大于0')
        if self.option('rm_host'):
            if self.option('ref_database') == '' and not self.option('ref_undefined').is_set:
                raise OptionError('已选择去宿主，需输入参考数据库或参考序列')
            if self.option('ref_database') != '' and self.option('ref_undefined').is_set:
                raise OptionError('去宿主不可同时提供参考数据库及参考序列')
        if not self.option('assemble_tool') in ['soapdenovo', 'idba']:
            raise OptionError('请检查拼接工具是否输入正确')
        if not self.option('assemble_type') in ['simple', 'multiple']:
            raise OptionError('拼接策略参数错误，应为simple或multiple')
        if self.option('assemble_tool') == 'soapdenovo' and self.option('assemble_type') == 'multiple':
            raise OptionError('不支持SOAPdenovo混拼流程')
        if self.option('min_contig') < 200 or self.option('min_contig') > 1000:
            raise OptionError('最小Contig长度参数超出范围200~1000')
        if self.option('min_gene') < 0:
            raise OptionError('最小基因长度参数不能为负')
        return True

    def get_json(self):
        f = open(self.json_path, 'r')
        json_dict = json.loads(f.read())
        return json_dict

    def set_step(self, event):
        if 'start' in event['data'].keys():
            event['data']['start'].start()
        if 'end' in event['data'].keys():
            event['data']['end'].finish()
        self.step.update()

    def set_run(self, opts, module, event, step):
        module.set_options(opts)
        module.on('start', self.set_step, {'start': step})
        module.on('end', self.set_step, {'end': step})
        module.on('end', self.set_output, event)
        module.run()

    def run_qc(self):
        pass

    def run_rm_host(self):
        opts = {
            'fastq_dir': self.qc_fastq,
            'fq_type': 'PSE',
            'ref_database': self.option('ref_database'),
            'ref_undefined': self.option('ref_undefined'),
        }
        self.set_run(opts, self.rm_host, 'rm_host', self.step.rm_host)
        '''
        self.rm_host.set_options(opts)
        self.rm_host.on('start', self.set_step, {'start': self.step.rm_host})
        self.rm_host.on('end', self.set_step, {'end': self.step.rm_host})
        self.rm_host.on('end', self.set_output, 'rm_host')
        self.rm_host.run()
        '''

    def run_assem(self):
        opts = {
            'data_id': self.option('main_id'),
            'QC_dir': self.rm_host.output_dir,  # self.rm_host.option('result_fq_dir'),
            'min_contig': self.option('min_contig'),
        }
        if self.option('assemble_tool') == "soapdenovo":
            self.set_run(opts, self.assem_soapdenovo, 'assem_soapdenovo', self.step.assem)
            '''
            self.assem_soapdenovo.set_options(opts)
            self.assem_soapdenovo.on('start', self.set_step, {'start': self.step.assem})
            self.assem_soapdenovo.on('end', self.set_step, {'end': self.step.assem})
            self.assem_soapdenovo.on('end', self.set_output, 'rm_host')
            '''
        else:
            opts['method'] = self.option('assemble_type')
            self.set_run(opts, self.assem_idba, 'assem_idba', self.step.assem)

    def run_gene_predict(self):
        opts = {
            'min_gene': str(self.option('min_gene')),
        }
        if self.option('assemble_tool') == "soapdenovo":
            opts['input_fasta'] = self.assem_soapdenovo.option('contig')
        else:
            opts['input_fasta'] = self.assem_idba.option('contig')
        self.set_run(opts, self.gene_predict, 'gene_predict', self.step.gene_predict)

    def run_gene_set(self):
        pass

    def run_gene_profile(self):
        pass

    def run_align(self, event):
        pass

    def run_annotation(self):
        pass


    '''处理输出文件'''

    def set_output(self, event):
        '''
        将各个模块的结果输出至output
        '''
        obj = event['bind_object']
        if event['data'] == 'rm_host':
            self.move_dir(obj.output_dir, 'rm_host')
        if event['data'] == 'assem':
            self.move_dir(obj.output_dir, 'assemble')
        if event['data'] == 'gene_predict':
            self.move_dir(obj.output_dir, 'predict')

    def set_output_all(self):
        """
        将所有结果一起导出至output
        """
        pass

    def move_dir(self, olddir, newname, mode='link'):  # 原函数名move2outputdir
        """
        移动一个目录下所有文件/文件夹到workflow输出路径下，供set_output调用
        """
        start = time.time()
        if not os.path.isdir(olddir):
            raise Exception('需要移动到output目录的文件夹不存在。')
        newdir = os.path.join(self.output_dir, newname)
        if not os.path.exists(newdir):
            os.makedirs(newdir)
        allfiles = os.listdir(olddir)
        oldfiles = [os.path.join(olddir, i) for i in allfiles]
        newfiles = [os.path.join(newdir, i) for i in allfiles]
        self.logger.info(newfiles)
        for newfile in newfiles:
            if os.path.isfile(newfile) and os.path.exists(newfile):
                os.remove(newfile)
            elif os.path.isdir(newfile) and os.path.exists(newfile):
                shutil.rmtree(newfile)
        for i in range(len(allfiles)):
            self.move_file(oldfiles[i], newfiles[i])
        end = time.time()
        duration = end - start
        self.logger.info("文件夹{}移动到{},耗时{}s".format(olddir, newdir, duration))

    def move_file(self, old_file, new_file):
        """
        递归移动文件夹的内容，供move_dir调用
        """
        if os.path.isfile(old_file):
            os.link(old_file, new_file)
        elif os.path.isdir(old_file):
            os.mkdir(new_file)
            for file in os.linkdir(old_file):
                file_path = os.path.join(old_file, file)
                new_path = os.path.join(new_file, file)
                self.move_file(file_path, new_path)
        else:
            self.logger.info("导出失败：请检查{}".format(old_file))

    def end(self):
        # self.run_api()
        self.set_upload_results()
        super(MetaGenomicWorkflow, self).end()

    def set_upload_results(self):  # 原modify_output
        """
        结果放置到/upload_results
        """
        pass

    '''导表'''

    def run_api(self, test=False):  # 原run_api_and_set_output
        greenlets_list_first = []  # 一阶段导表
        greenlets_list_sec = []  # 二阶段导表
        greenlets_list_third = []  # 三阶段导表

    def export_test(self):
        self.api_qc = XXX
        self.group_id = ObjectId(XX)
        self.api_qc.add_control_group(XXX)

    @time_count
    def export_XXX(self):
        pass

    def run(self):
        """
        运行 meta_genomic workflow
        :return:
        """
        self.IMPORT_REPORT_DATA = True
        self.IMPORT_REPORT_DATA_AFTER_END = False
        task_info = self.api.api('task_info.ref')
        task_info.add_task_info()
        self.rm_host.on('end', self.run_assem)
        self.assem_soapdenovo.on('end', self.run_gene_predict)
        self.assem_idba.on('end', self.run_gene_predict)
        self.gene_predict.on('end', self.end)
        # '''
        if self.option('rm_host'):
            self.run_rm_host()
        else:
            self.run_assem()
        # '''
        super(MetaGenomicWorkflow, self).run()


