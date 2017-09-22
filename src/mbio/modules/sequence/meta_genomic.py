#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__ == zhouxuan

import os
from biocluster.core.exceptions import OptionError
from biocluster.module import Module


class MetaGenomicModule(Module):
    """
    对数据进行解压统计碱基质量
    """
    def __init__(self, work_id):
        super(MetaGenomicModule, self).__init__(work_id)
        options = [
            {"name": "fastq_dir", "type": "infile", "format": "paternity_test.data_dir"},
        ]
        self.add_option(options)
        self.base_info = self.add_tool("meta.qc.base_info")
        self.tools = []

    def check_options(self):
        """
        检查参数
        """
        if not self.option("fastq_dir").is_set:
            raise OptionError("必须输入样本文件夹！")
        else:
            return True

    def run(self):
        super(MetaGenomicModule, self).run()
        self.run_ungiz()

    def run_ungiz(self):
        file_name = os.listdir(self.option('fastq_dir').prop['path'])
        reslut_path = os.path.join(self.work_dir, "ungiz_dir")
        if not os.path.exists(reslut_path):
            os.mkdir(reslut_path)
        for i in file_name:
            file_path = os.path.join(self.option('fastq_dir').prop['path'], i)
            gunzip_fastq = self.add_tool('sequence.fastq_ungz')
            gunzip_fastq.set_options({
                "fastq": file_path,
                "result_path": reslut_path
            })
            self.tools.append(gunzip_fastq)
        if len(self.tools) > 1:
            self.on_rely(self.tools, self.run_base_info)
        else:
            self.tools[0].on('end', self.run_base_info)
        for tool in self.tools:
            tool.run()

    def run_base_info(self):
        self.base_info.set_options({"fastq_path": os.path.join(self.work_dir, "ungiz_dir")})
        self.base_info.on('end', self.set_output)
        self.base_info.run()

    def set_output(self):
        if os.path.exists(self.base_info.output_dir + "/base_info"):
            try:
                self.linkdir(self.base_info.output_dir + "/base_info", "base_info")
            except Exception, e:
                raise Exception('base_info的结果linkdir时出错{}'.format(e))
        self.end() #modified by zouxuan 之前脚本无法自动结束

    def linkdir(self, dirpath, dirname):
        """
		link一个文件夹下的所有文件到本module的output目录
		:param dirpath: 传入文件夹路径
		:param dirname: 新的文件夹名称
		:return:
		"""
        allfiles = os.listdir(dirpath)
        newdir = os.path.join(self.output_dir, dirname)
        if not os.path.exists(newdir):
            os.mkdir(newdir)
        oldfiles = [os.path.join(dirpath, i) for i in allfiles]
        newfiles = [os.path.join(newdir, i) for i in allfiles]
        for newfile in newfiles:
            if os.path.exists(newfile):
                if os.path.isfile(newfile):
                    os.remove(newfile)
                else:
                    os.system('rm -r %s' % newfile)
        for i in range(len(allfiles)):
            if os.path.isfile(oldfiles[i]):
                os.link(oldfiles[i], newfiles[i])
            elif os.path.isdir(oldfiles[i]):
                file_name = os.listdir(oldfiles[i])
                os.mkdir(newfiles[i])
                for file_name_ in file_name:
                    os.link(os.path.join(oldfiles[i], file_name_), os.path.join(newfiles[i], file_name_))