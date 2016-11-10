# -*- coding:utf-8 -*-
# __author__
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import shutil
import os
import re
import subprocess

class HtseqAgent(Agent):

    def __init__(self, parent):
        super(HtseqAgent, self).__init__(parent)
        options = [
            {"name": "bam", "type": "infile", "format": "align.bwa.bam"},  # 样本比对后的bam文件
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.reads_mapping.gtf"},  # 参考基因组的gtf文件
            {"name": "strand_specific", "type": "string", "default":"None"},  # PE测序，是否链特异性, 默认是, 无特异性
            {"name": "strand_dir", "type": "string"},  # 链特异性时选择正链, "reverse","forward"
            {"name": "sort_type", "type": "string", "default": "pos"},  # 按照位置排序 "name"
            {"name": "htseq_count","type":"outfile","format": "denovo_rna.express.express_matrix"},
            {"name": "gtf_type", "type":"string","default": "ref"},  #ref，merge_cufflinks，merge_stringtie三种参数
            #{"name": "sam", "type":"outfile","format": "align.bwa.sam"},
            {"name": "cpu", "type": "int", "default": 4},  #设置CPU
            {"name": "max_memory", "type": "string", "default": "100G"}  #设置内存
        ]
        self.add_option(options)
        self.step.add_steps("htseq")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.htseq.start()
        self.step.update()

    def stepfinish(self):
        self.step.htseq.finish()
        self.step.update()

    def check_options(self):
        if self.option("strand_specific") == True:
            if not self.option("firststrand") and not self.option("secondstrand"):
                raise OptionError("链特异性时需要选择正链或者负链")
        if not self.option("ref_gtf").is_set:
            raise OptionError("需要输入gtf文件")
        if not self.option('bam').is_set:
            raise OptionError('需要输入bam文件')
        return True

    def set_resource(self):
        self._cpu = 4
        self._memory = '100G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"]
        ])
        result_dir.add_regexp_rules([
            [r"htseq", "", "基因的count值"],
            [r'sam','','生成sam文件']
        ])
        super(HtseqAgent, self).end()

class HtseqTool(Tool):

    def __init__(self, config):
        super(HtseqTool, self).__init__(config)
        self._version = '1.0.1'
        self.gcc = self.config.SOFTWARE_DIR + '/gcc/5.1.0/bin'
        self.gcc_lib = self.config.SOFTWARE_DIR + '/gcc/5.1.0/lib64'
        #os.path.join(Config().SOFTWARE_DIR, "program/R-3.3.1/bin/R")
        self.htseq_path = os.path.join(Config().SOFTWARE_DIR, 'bioinfo/rna/HTSeq-0.6.1/scripts/htseq-count')
        self.gtf_path = os.path.join(Config().SOFTWARE_DIR, 'bioinfo/rna/scripts/draw_htseq.py ')
        #'/mnt/ilustre/users/sanger-dev/app/bioinfo/rna/scripts/draw_htseq.py '
        self.Python_path ='program/Python/bin/python '
        self.samtools_path = 'bioinfo/align/samtools-1.3.1/samtools'
        self.set_environ(PATH=self.gcc, LD_LIBRARY_PATH=self.gcc_lib)
        self.set_environ(PATH = self.htseq_path)

    def  bam_to_sam_run(self, file_path):
         path_split = os.path.split(file_path)[0]
         sample_name = os.path.split(file_path)[1].split('.bam')[0]
         sam_path = os.path.join(path_split, sample_name+'.sam')
         cmd = self.samtools_path + " view -h {} -o {}".format(file_path, sam_path)
         self.logger.info('把bam文件转为sam')
         bam_to_sam_cmd = self.add_command('bam_to_sam', cmd).run()
         self.wait()
         if bam_to_sam_cmd.return_code == 0:
             self.logger.info('%s运行完成' % bam_to_sam_cmd)
         else:
             self.set_error("%s运行出错" % cmd)

    def htseq_run(self):
        file_type = check_file_format(file_path=self.option("bam").prop['path'])
        if file_type:
            sample_path = self.option('bam').prop['path']
            sample_name = os.path.split(sample_path)[1].split('.sam')[0]
        else:
            sample_type = os.path.basename(self.option("bam").prop['path'])
            sample_name = sample_type.split(".sam")[0]
            self.logger.info(output_dir)
            for root, dirs, files in os.walk(self.work_dir):
                for f in files:
                    if f.find('.sam') != -1:
                        sample_path = os.path.join(self.work_dir, f)
                    else:
                        raise NameError('没有生成sam文件，无法进行htseq表达量计算！')
        output_dir = os.path.join(self.work_dir, sample_name)
        self.logger.info("开始运行htseq计算表达量")
        if self.option("strand_specific") == "None":
            cmd = self.htseq_path + " %s %s -r %s -s %s -q > %s" % (sample_path, self.option("ref_gtf").prop['path'],
                          self.option("sort_type"), "no", output_dir)
        else:
            cmd = self.htseq_path + " %s %s -r %s -s %s -q > %s" % (sample_path, self.option("ref_gtf").prop['path'],
                          self.option("sort_type"), self.option("strand_dir"), output_dir)
        try:
            subprocess.check_call(cmd, shell=True)
            self.logger.info(cmd)
            self.logger.info('htseq计算表达量程序运行完成！')
        except subprocess.CalledProcessError:
            self.logger.info("htseq计算表达量程序运行失败{}".format(cmd))
            raise Exception("htseq计算表达量程序运行失败{}".format(cmd))
        #htseq_cmd = self.add_command("htseq", cmd).run()
        self.wait()
        #if htseq_cmd.return_code == 0:
        #    self.logger.info("%s运行完成" % htseq_cmd)
        #else:
        #    self.set_error("%s运行出错" % cmd)

    def gtf_run(self):
        """从gtf文件中提取基因的信息，并生成新的文件，留给客户查看"""
        for root,dirs,files in os.walk(self.work_dir):
            for f in files:
                if f.find('sample') != -1:
                    file_path =os.path.join(self.work_dir,f)
        if self.option('gtf_type').find('stringtie') != -1:
            gtf_path = os.path.split(self.option('ref_gtf').prop['path'])[0]
            gtf_name = os.path.split(self.option('ref_gtf').prop['path'])[1].split('.gtf')
            new_gtf_path = os.path.join(gtf_path,gtf_name+'.gtf')
            os.system("grep '#' -v {} > {}".format(self.option('gtf_type').prop['path'],new_gtf_path))
            gtf_cmd = self.Python_path + self.gtf_path+'-s {} -gtf {} -p {} -gtf_type {}'.format(file_path, new_gtf_path, self.work_dir, self.option('gtf_type'))
        else:
            gtf_cmd = self.Python_path + self.gtf_path+'-s {} -gtf {} -p {} -gtf_type {}'.format(file_path, self.option('ref_gtf').prop['path'], self.work_dir, self.option('gtf_type'))
        htq_cmd = self.add_command("htseq", gtf_cmd).run()
        self.wait()
        if htq_cmd.return_code == 0:
            self.logger.info('%s运行完成'%htq_cmd)
        else:
            self.set_error('%s运行出错' %gtf_cmd)

    def set_output(self):
        self.logger.info("设置结果目录")
        sample_name=os.path.basename(self.option("bam").prop['path']).split(".bam")[0]
        self.logger.info(self.work_dir)
        try:
            """
            for root,dirs,files in os.walk(self.work_dir):
                for f in files:
                    if f.find('sample') != -1:
                        #new_name=os.rename(f, sample_name+"_"+f)
                        file_name=os.path.join(self.work_dir, f)
                        shutil.copy2(file_name, os.path.join(self.output_dir,f))
                        self.option("htseq_count").set_path(file_name)
                        self.logger.info(file_name)
            """
            gtf_path= os.path.split(self.work_dir)[0]
            self.logger.info(gtf_path)
            for root,dirs,files in os.walk(gtf_path):
                for f in files:
                    if f.find('_gtf.txt') != -1:
                        gtf_file=os.path.join(gtf_path, f)
                        self.logger.info(gtf_file)
                        shutil.copy2(gtf_file, os.path.join(self.output_dir,f))
                        self.option('htseq_count').set_path(gtf_file)
            self.logger.info("设置htseq分析结果目录成功")
        except Exception as e:
            self.logger.info("设置htseq分析结果目录失败{}".format(e))
            self.set_error("设置htseq分析结果目录失败{}".format(e))

    def run(self):
        super(HtseqTool, self).run()
        if check_file_format(self.option('bam').prop['path']):
            self.htseq_run()
        else:
            self.bam_to_sam_run(self.option('bam').prop['path'])
            self.htseq_run()
        self.gtf_run()
        self.set_output()
        self.end()

def check_file_format(file_path):
    """检查输入文件的格式——sam或者bam文件"""
    sample_type = os.path.basename(file_path)
    if sample_type.find('.sam') != -1:
        return True
    else:
        return False
