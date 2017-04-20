# -*- coding:utf-8 -*-
# __author__ = 'shijin'
# last_modified by shijin
"""本地基因组注释用工作流"""

from biocluster.workflow import Workflow
from biocluster.core.exceptions import OptionError, FileError
import os
import json
import shutil


class AnnoWorkflow(Workflow):
    def __init__(self, wsheet_object):
        """
        有参workflow option参数设置
        """
        self._sheet = wsheet_object
        super(AnnoWorkflow, self).__init__(wsheet_object)
        options = [
            {"name": "nr_out", "type": "infile", "format": "align.blast.blast_xml"},
            {"name": "kegg_out", "type": "infile", 'format': "align.blast.blast_xml"},
            {"name": "string_out", "type": "infile", "format": "align.blast.blast_xml"},
            {"name": "database", "type": "string", "format": "go,nr,cog,kegg"},
            {"name": "gene_file", "type": "infile", "format": "denovo_rna.express.gene_list"}
        ]
        self.add_option(options)
        self.set_options(self._sheet.options())
        self.json_path = self.config.SOFTWARE_DIR + "/database/refGenome/scripts/ref_genome.json"
        self.json = self.get_json()
        self.annotation = self.add_module('annotation.ref_annotation')
        self.change_tool = self.add_tool("align.diamond.change_diamondout")
        self.step.add_steps("qcstat", "mapping", "assembly", "annotation", "exp_ref", "exp_new_transcripts",
                            "exp_new_genes", "exp_merged", "map_stat",
                            "seq_abs", "transfactor_analysis", "network_analysis", "sample_analysis",
                            "altersplicing")

    def check_options(self):
        """
        检查选项
        """
        return True

    def set_step(self, event):
        if 'start' in event['data'].keys():
            event['data']['start'].start()
        if 'end' in event['data'].keys():
            event['data']['end'].finish()
        self.step.update()

    def get_json(self):
        f = open(self.json_path, "r")
        json_dict = json.loads(f.read())
        return json_dict

    def test_run(self):
        self.change_tool.on("end", self.run_annotation)
        self.annotation.on("end", self.end)
        self.run_change_diamond()
        super(AnnoWorkflow, self).run()

    def run_change_diamond(self):
        opts = {
            "nr_out": self.option('nr_out'),
            "kegg_out": self.option('kegg_out'),
            "string_out": self.option('string_out')
        }

        self.change_tool.set_options(opts)
        self.change_tool.run()

    def run_annotation(self):
        anno_opts = {
            "gene_file": self.option("gene_file"),
            'go_annot': True,
            'blast_nr_xml': self.change_tool.option('blast_nr_xml')
        }
        if 'nr' in self.option('database'):
            anno_opts.update({
                'nr_annot': True,
                'blast_nr_xml': self.change_tool.option('blast_nr_xml'),
            })
        else:
            anno_opts.update({'nr_annot': False})
        if 'kegg' in self.option('database'):
            anno_opts.update({
                'blast_kegg_xml': self.change_tool.option('blast_kegg_xml'),
            })
        if 'cog' in self.option('database'):
            anno_opts.update({
                'blast_string_xml': self.change_tool.option('blast_string_xml'),
            })
        self.logger.info('....anno_opts:%s' % anno_opts)
        self.annotation.set_options(anno_opts)
        self.annotation.on('end', self.set_output, 'annotation')
        self.annotation.on('end', self.set_step, {'end': self.step.annotation})
        self.annotation.run()

    def move2outputdir(self, olddir, newname, mode='link'):
        """
        移动一个目录下的所有文件/文件夹到workflow输出文件夹下
        """
        if not os.path.isdir(olddir):
            raise Exception('需要移动到output目录的文件夹不存在。')
        newdir = os.path.join(self.output_dir, newname)
        if not os.path.exists(newdir):
            if mode == 'link':
                shutil.copytree(olddir, newdir, symlinks=True)
            elif mode == 'copy':
                shutil.copytree(olddir, newdir)
            else:
                raise Exception('错误的移动文件方式，必须是\'copy\'或者\'link\'')
        else:
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
                if os.path.isfile(oldfiles[i]):
                    os.system('cp {} {}'.format(oldfiles[i], newfiles[i]))
                else:
                    os.system('cp -r {} {}'.format(oldfiles[i], newdir))

    def set_output(self, event):
        obj = event["bind_object"]
        # 设置qc报告文件
        if event['data'] == 'qc':
            self.move2outputdir(obj.output_dir, 'QC_stat')
        if event['data'] == 'qc_stat_before':
            self.move2outputdir(obj.output_dir, 'QC_stat/before_qc')
            self.logger.info('{}'.format(self.qc_stat_before._upload_dir_obj))
        if event['data'] == 'qc_stat_after':
            self.move2outputdir(obj.output_dir, 'QC_stat/after_qc')
            self.logger.info('{}'.format(self.qc_stat_after._upload_dir_obj))
        if event['data'] == 'mapping':
            self.move2outputdir(obj.output_dir, 'mapping')
            self.logger.info('mapping results are put into output dir')
        if event['data'] == 'map_qc':
            self.move2outputdir(obj.output_dir, 'map_qc')
            self.logger.info('mapping assessments are done')
        if event['data'] == 'assembly':
            self.move2outputdir(obj.output_dir, 'assembly')
            self.logger.info('assembly are done')
        if event['data'] == 'express_ref':
            self.move2outputdir(obj.output_dir, 'express_ref')
            self.logger.info('未拼接express文件移动完成')
        if event['data'] == 'express_new_transcripts':
            self.move2outputdir(obj.output_dir, 'express_new_transcripts')
            self.logger.info('新转录本express文件移动完成')
        if event['data'] == 'express_new_genes':
            self.move2outputdir(obj.output_dir, 'express_new_genes')
            self.logger.info('新基因express文件移动完成')
        if event['data'] == 'express_merged':
            self.move2outputdir(obj.output_dir, 'express_merged')
            self.logger.info('拼接转录本express文件移动完成')
        if event['data'] == 'exp_diff':
            self.move2outputdir(obj.output_dir, 'express_diff')
            self.logger.info("express diff")
        if event['data'] == 'snp_rna':
            self.move2outputdir(obj.output_dir, 'snp_rna')
            self.logger.info("snp_rna文件移动完成")
        if event['data'] == 'network_analysis':
            self.move2outputdir(obj.output_dir, 'network_analysis')
            self.logger.info("network_analysis文件移动完成")
        if event['data'] == 'tf':
            self.move2outputdir(obj.output_dir, 'transfactor_analysis')
            self.logger.info("transfactor_analysis文件移动完成")
        if event['data'] == 'annotation':
            self.move2outputdir(obj.output_dir, 'annotation')
            self.logger.info("annotation文件移动完成")
        if event['data'] == 'altersplicing':
            self.move2outputdir(obj.output_dir, 'altersplicing')
            self.logger.info("altersplicing文件移动完成")

    def run(self):
        """
        ref-rna workflow run方法
        :return:
        """
        self.run_change_diamond()
        super(AnnoWorkflow, self).run()

    def end(self):
        super(AnnoWorkflow, self).end()
