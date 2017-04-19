# -*- coding:utf-8 -*-
# __author__: konghualei 20170418
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
import shutil
import os
import re


class FeaturecountsAgent(Agent):
    """featurecounts 软件计算基因的表达量，支持bam文件夹格式的输入"""
    def __init__(self, parent):
        super(FeaturecountsAgent, self).__init__(parent)
        options = [
            {"name": "fq_type", "type": "string","default": "PE"},  # PE OR SE
            {"name": "ref_gtf", "type": "infile", "format": "ref_rna.reads_mapping.gtf"},  # 参考基因组的gtf文件
            {"name": "bam", "type": "infile", "format": "align.bwa.bam"},  # 样本比对后的bam文件
            {"name": "strand_specific", "type": "string", "default": "None"},  # PE测序，是否链特异性, 默认是0, 无特异性
            {"name": "strand_dir", "type": "string","default": "None"},  # "forward", "reverse" 默认不设置此参数
            #{"name": "secondstrand", "type": "int", "default": 2},  # 链特异性时选择负链, 默认不设置此参数
            {"name": "feature_id", "type": "string", "default": "gene_id"},  # 默认计算基因的count值，可以选择exon，both等
            #{"name": "summary", "type":"outfile", "format": "ref_rna.summary"},  # featurecounts 结果输出
            {"name": "count_data", "type":"outfile", "format": "denovo_rna.express.express_matrix"},  # featurecounts 输出结果总结
            {"name": "cpu", "type": "int", "default": 10},  #设置CPU
            {"name": "max_memory", "type": "string", "default": "100G"}  #设置内存
        ]
        self.add_option(options)
        self.step.add_steps("featurecounts")
        self.on('start', self.stepstart)
        self.on('end', self.stepfinish)

    def stepstart(self):
        self.step.featurecounts.start()
        self.step.update()

    def stepfinish(self):
        self.step.featurecounts.finish()
        self.step.update()

    def check_options(self):
        if not self.option('fq_type'):
            raise OptionError('必须设置测序类型：PE OR SE')
        if self.option('fq_type') not in ['PE', 'SE']:
            raise OptionError('测序类型不在所给范围内')
        if self.option("strand_specific") == 1:
            if not self.option("firststrand") and not self.option("secondstrand"):
                raise OptionError("链特异性时需要选择正链或者负链")
        if not self.option("ref_gtf").is_set:
            raise OptionError("需要输入gtf文件")
        if self.option("feature_id") not in ["gene_id","exon","both"]:
            raise OptionError("计算基因或者外显子的count值")
        return True

    def set_resource(self):
        self._cpu = 10
        self._memory = '100G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "summary", "结果输出目录"]
        ])
        result_dir.add_regexp_rules([
            #[r"featurecounts+\.summary", "summary", "summary 记录"],
            #[r"featurecounts+\^sample\d", "","gene count 表"]
        ])
        super(FeaturecountsAgent, self).end()

class FeaturecountsTool(Tool):

    def __init__(self, config):
        super(FeaturecountsTool, self).__init__(config)
        self._version = '1.0.1'
        self.featurecounts_path = '/bioinfo/align/subread-1.5.0/bin/featureCounts'
        self.set_environ(PATH = self.featurecounts_path)

    def featurecounts_run(self):
        sample_name=os.path.basename(self.option("bam").prop['path']).split('.bam')[0]
        output_dir=os.path.join(self.work_dir, sample_name)
        if "PE" in self.option('fq_type'):
            if self.option("strand_specific") != "None":
                if self.option("strand_dir").find("forward") != -1:
                    cmd = self.featurecounts_path + " -T %s -p -a %s -g %s -M -s %s -o %s %s" % (bam, self.option('cpu'), self.option("ref_gtf").prop['path'],
                        self.option("feature_id"), 1, output_dir, self.option("bam").prop['path'])
                if self.option("strand_dir").find("reverse") != -1:
                    cmd = self.featurecounts_path + " -T %s -p -a %s -g %s -M -s %s -o %s %s" % (bam, self.option('cpu'), self.option("ref_gtf").prop['path'],
                        self.option("feature_id"), 2, output_dir, self.option("bam").prop['path'])
            else:
                    cmd = self.featurecounts_path + " -T %s -p -a %s -g %s -M -s %s -o %s %s" % (self.option('cpu'), self.option("ref_gtf").prop['path'],
                        self.option("feature_id"), 0, output_dir, self.option("bam").prop['path'])
        else:
            cmd = self.featurecounts_path + " -T %s -a %s -g %s -M -s %s -o %s %s" % (self.option('cpu'), self.option("ref_gtf").prop['path'],
                    self.option("feature_id"), 0, output_dir, self.option("bam").prop['path'])
        self.logger.info("开始运行featureCounts计算表达量")
        featurecounts_cmd = self.add_command("featurecounts", cmd).run()
        self.wait()
        if featurecounts_cmd.return_code == 0:
            self.logger.info("%s运行完成" % featurecounts_cmd)
        else:
            self.set_error("%s运行出错" % cmd)


    def set_output(self):
        self.logger.info("设置结果目录")
        sample_name=os.path.basename(self.option("bam").prop['path']).split(".bam")[0]
        self.logger.info(sample_name)
        try:
            for root, dirs, files in os.walk(self.work_dir):
                for names in files:
                    if names.find(sample_name) != -1:
                        if names.find("summary") == -1:
                            filename = os.path.join(self.work_dir, names)
                            remove_header(file_path = filename, file_name = names)
                            path = os.path.join(os.path.split(filename)[0], names)
                            self.logger.info(path)
                            shutil.copy2(path, os.path.join(self.output_dir, names))
                            self.logger.info(os.path.join(self.output_dir, names))
                            self.option("count_data").set_path(filename)
                            self.logger.info("count success")
            for file in os.listdir(self.output_dir):
                self.logger.info(file)
                if file.find("summary") != -1:
                    os.remove(file)
            self.logger.info("设置featurecounts分析结果目录成功")
        except Exception as e:
            self.logger.info("设置featurecounts分析结果目录失败{}".format(e))

    def run(self):
        super(FeaturecountsTool, self).run()
        self.featurecounts_run()
        self.set_output()
        self.end()

def remove_header(file_path, file_name):
    """除去生成文件的header标签，同时将最后一列列名更新为count"""
    if os.path.exists(file_path):
        file_path_path = os.path.split(file_path)[0]
        new_file_path = os.path.join(file_path_path, "count")
        os.system('grep \"#\" -v {} > {}'.format(file_path, new_file_path))
        output_file_path = os.path.join(file_path_path, file_name)
        file1=open(output_file_path, 'w+')
        with open(new_file_path, 'r+') as files:
            i = 0
            for file in files:
                i += 1
                line = file.strip().split("\t")
                for f in range(len(line)):
                    if f != len(line) - 1:
                        file1.write(line[f] + "\t")
                    else:
                        if i == 1:
                            file1.write("count" +"\n")
                        else:
                            file1.write(line[f] + "\n")
        os.remove(new_file_path)
        file1.close()
        if os.path.exists(output_file_path):
             pass
    else:
        raise Exception("输入文件不存在，无法除掉文件标签！")
