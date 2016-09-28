# -*- coding: utf-8 -*-
# __author__ = 'qindanhua'
from biocluster.workflow import Workflow
from mbio.api.to_file.denovo import *


class MapAssessmentWorkflow(Workflow):
    """
    报告中计算稀释性曲线时使用
    """

    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        super(MapAssessmentWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "bed", "type": "infile", "format": "denovo_rna.gene_structure.bed"},  # bed格式文件
            {"name": "bam", "type": "infile", "format": "align.bwa.bam,align.bwa.bam_dir"},  # bam格式文件,排序过的
            {"name": "fpkm", "type": "infile", "format": "denovo_rna.express.express_matrix"},  # 基因表达量表
            {"name": "update_info", "type": "string"},
            {"name": "analysis", "type": "string"},  # 分析类型
            {"name": "quality_satur", "type": "int"},  # 测序饱和度分析质量值
            {"name": "quality_dup", "type": "int"},  # 冗余率分析质量值
            {"name": "low_bound", "type": "int"},  # Sampling starts from this percentile
            {"name": "up_bound", "type": "int"},  # Sampling ends at this percentile
            {"name": "step", "type": "int"},  # Sampling frequency
            {"name": "min_len", "type": "int"}  # Minimum mRNA length (bp).
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.map_assess = self.add_module('denovo_rna.gene_structure.map_assessment')

    def run(self):
        options = {
            'bed': self.option('bed'),
            'bam': self.option('bam'),
            'analysis': self.option('analysis'),
            'quality_satur': self.option('quality_satur'),
            'quality_dup': self.option('quality_dup'),
            'low_bound': self.option('low_bound'),
            'up_bound': self.option('up_bound'),
            'step': self.option('step'),
            'fpkm': self.option('fpkm'),
            'min_len': self.option('min_len')
            }
        self.map_assess.set_options(options)
        self.map_assess.on('end', self.set_db)
        self.map_assess.run()
        self.output_dir = self.map_assess.output_dir
        super(MapAssessmentWorkflow, self).run()

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "dir", "结果输出目录"],
            ["./coverage/", "dir", "基因覆盖度分析输出目录"],
            ["./dup/", "dir", "冗余序列分析输出目录"],
            ["./satur/", "dir", "测序饱和度分析输出目录"],
            ["./bam_stat.xls", "xls", "bam格式比对结果统计表"]
        ])
        result_dir.add_regexp_rules([
            [r".*pos\.DupRate\.xls", "xls", "比对到基因组的序列的冗余统计表"],
            [r".*seq\.DupRate\.xls", "xls", "所有序列的冗余统计表"],
            [r".*eRPKM\.xls", "xls", "RPKM表"],
            [r".*cluster_percent\.xls", "xls", "饱和度作图数据"],
            [r".correlation_matrix*\.xls", "xls", "相关系数矩阵"],
            [r".hcluster_tree*\.xls", "xls", "样本间相关系数树文件"]
        ])
        # print self.get_upload_files()
        super(MapAssessmentWorkflow, self).end()
