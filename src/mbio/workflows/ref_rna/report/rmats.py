# -*- coding: utf-8 -*-
# __author__ = fiona
# time: 2017/5/17 08:57

import re, os, Bio, argparse, sys, fileinput

import json
from mainapp.controllers.project.meta_controller import MetaController
from mainapp.libs.param_pack import group_detail_sort
from bson import ObjectId
import datetime
import pandas as pd
import shutil, subprocess
import re
from biocluster.workflow import Workflow
import importlib
import os
import shutil
from biocluster.core.exceptions import OptionError
from biocluster.module import Module
from mbio.packages.gene_structure.rmats_process_func import *
from mbio.packages.gene_structure.rmats_process_func import process_single_rmats_output_dir
import re
from mbio.files.sequence.file_sample import FileSampleFile


class RmatsWorkflow(Workflow):
    '''
    '''
    
    def __init__(self, wsheet_object):
        self._sheet = wsheet_object
        # self.logger.info(wsheet_object.options)
        self.rpc = False
        super(RmatsWorkflow, self).__init__(wsheet_object)
        options = [
            # {"name": "sample_bam_dir", "type": "infile", "format": "align.bwa.bam_dir"},
            # {"name": "rmats_control", "type": "infile", "format": "sample.control_table"},
            # {"name": "group_table", "type": "string"},
            # {"name": "group_detail", "type": "string"},
            # {"name": "gname", "type": "string", "default": "group"},  # 分组方案名称
            {"name": "seq_type", "type": "string", "default": "paired"},  # 两个选项：'paired'  or ’single‘
            {"name": "analysis_mode", "type": "string", "default": "P"},
            {"name": "read_length", "type": "int", "default": 160},
            {"name": "ref_gtf", "type": "string"},  # 一定要设置
            {"name": "novel_as", "type": "int", "default": 1},  # 是否发现新的AS事件，默认为是
            {"name": "lib_type", "type": "string", "default": "fr-unstranded"},  # 建库类型
            {"name": "cut_off", "type": "float", "default": 0.05},
            {"name": "keep_temp", "type": "int", "default": 0},
            {"name": "update_info", "type": "string"},
            # {"name": "chr_set", "type": "string"},
            {"name": "case_group_bam_str", "type": "string"},
            {"name": "control_group_bam_str", "type": "string"},
            {"name": "case_group_name", "type": "string"},
            {"name": "control_group_name", "type": "string"},
            {"name": "splicing_id", "type": "string"},
            {"name": "control_id", "type": "string"},
            {"name": "control_file", "type": "string"},
            {"name": "group_id", "type": "string"},
            {"name": "group_detail", "type": "string"}
            # {"name":"ref_gtf",}
            # {"name": "case_name", "type": "string"},
            # {"name": "control_name", "type": "string"}
        ]
        self.logger.info(options)
        self.add_option(options)
        self.set_options(self._sheet.options())
    
    def check_options(self):
        pass
    
    def run_rmats(self):
        opts = {
            "A_group_bam": self.option("case_group_bam_str"),
            "B_group_bam": self.option("control_group_bam_str"),
            "lib_type": self.option("lib_type"),
            "ref_gtf": self.option("ref_gtf"),
            "seq_type": self.option("seq_type"),
            "read_length": self.option("read_length"),
            "novel_as": self.option("novel_as"),
            "cut_off": self.option("cut_off"),
            "keep_temp": self.option("keep_temp"),
            "analysis_mode": self.option("analysis_mode")
        }
        self.rmats = self.add_tool("gene_structure.rmats_bam")
        self.rmats.set_options(opts)
        self.rmats.on("end", self.set_output)
        self.rmats.run()
    
    def run(self):
        self.run_rmats()
        super(RmatsWorkflow, self).run()
    
    def set_output(self):
        self.logger.info('set output')
        
        output_dir = os.path.join(self.output_dir, self.rmats.name)
        if not os.path.exists(output_dir):
            os.mkdir(output_dir)
        outfiles = os.listdir(self.rmats.output_dir)
        for f in outfiles:
            f_path = os.path.join(self.rmats.output_dir, f)
            target = os.path.join(output_dir, f)
            os.symlink(f_path, target)
        # self.set_db()
        self.end()
    
    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [r'.', '', 'rmats结果输出目录']
        ]
        )
        result_dir.add_regexp_rules([
            ["fromGTF\.(RI|A3SS|A5SS|SE|MXE)\.alter_id\.txt", 'txt', '可变剪接事件基本表'],
            ["fromGTF\.novelEvents\.(RI|A3SS|A5SS|SE|MXE)\.alter_id\.txt", 'txt', '新发现可变剪接事件基本表'],
            ["(RI|A3SS|A5SS|SE|MXE)\.MATS\.ReadsOnTargetAndJunctionCounts\.alter_id\.psi_info\.txt", 'txt',
             '差异事件详情表（ReadsOnTargetAndJunctionCounts证据）'],
            ["(RI|A3SS|A5SS|SE|MXE)\.MATS\.JunctionCountOnly\.alter_id\.psi_info\.txt", 'txt',
             '差异事件详情表（JunctionCountOnly证据）'],
            ['all_events_detail_big_table.txt', 'txt', '全部结果整合大表'],
            ['config.txt', 'txt', '运行配置详情文件'],
            ['all_events_detail_big_table.txt', 'txt', '结果综合详情表']
        ])
        super(RmatsWorkflow, self).end()
    
    def set_db(self):
        """
        保存结果表保存到mongo数据库中
        """
        api_rmats_model = self.api.refrna_splicing_rmats
        self.logger.info("准备开始向mongo数据库中导入rmats分析的信息！")
        rmats_out_root_dir = self.rmats.output_dir
        group = {self.option('case_group_name'): "s1", self.option('control_group_name'): "s2"}
        # chr_set = [chr.strip() for chr in subprocess.check_output(
        #     "grep -P -V %s | awk -F '\\t'  '{print $1}' | uniq| sort | uniq " % self.option('ref_gtf').path,
        #     shell=True).strip().split('\n')]
        
        api_rmats_model.add_sg_splicing_rmats(params=self._options, major=True, group=group,
                                              ref_gtf=self.option('ref_gtf').path, outpath=rmats_out_root_dir)
        self.logger.info("向mongo数据库中导入rmats的信息成功！")
        self.end()
    
    def move2outputdir(self, olddir, newname, mode='link'):
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
