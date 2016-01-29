# -*- coding: utf-8 -*-
# __author__ = 'xuting'
import os
import re
import subprocess
import shutil
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
from biocluster.config import Config
from mbio.files.meta.otu.otu_table import OtuTableFile


class OtuTaxonStatAgent(Agent):
    """
    version 1.0
    author: xuting
    last_modify: 2015.11.03
    """
    def __init__(self, parent):
        super(OtuTaxonStatAgent, self).__init__(parent)
        options = [
            {'name': 'in_otu_table', 'type': 'infile', 'format': 'meta.otu.otu_table'},  # 输入的otu表
            {'name': 'taxon_file', 'type': 'infile', 'format': 'taxon.seq_taxon'},  # 输入的taxon文件
            {'name': 'otu_taxon_biom', 'type': 'outfile', 'format': 'meta.otu.biom'},  # 输出的biom文件
            {'name': 'otu_taxon_table', 'type': 'outfile', 'format': 'meta.otu.otu_table'},  # 输出的otu表文件
            {'name': 'otu_taxon_dir', 'type': 'outfile', 'format': 'meta.otu.tax_summary_dir'}]  # 输出的otu_taxon_dir文件夹
        self.add_option(options)
        self.step.add_steps('OtuTaxonStat')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.OtuTaxonStat.start()
        self.step.update()

    def step_end(self):
        self.step.OtuTaxonStat.finish()
        self.step.update()

    def check_options(self):
        """
        参数检测
        """
        if not self.option("in_otu_table").is_set:
            raise OptionError("参数in_otu_table不能为空")
        if not self.option("taxon_file").is_set:
            raise OptionError("参数taxon_file不能为空")
        self.option("in_otu_table").get_info()
        if self.option("in_otu_table").prop['metadata'] == "taxonomy":
            raise OptionError("otu表不应该有taxonomy信息")

        return True

    def set_resource(self):
        """
        设置所需要的资源
        """
        self._cpu = 1
        self._memory = ''


class OtuTaxonStatTool(Tool):
    """
    otu taxon stat tool
    需要软件biom
    需要脚本make_otu_table.py,summarize_taxa.py,sum_tax.pl,
    """
    def __init__(self, config):
        super(OtuTaxonStatTool, self).__init__(config)
        self._version = 1.0
        self._biom_path = "Python/bin/biom"
        self._make_otu_table_path = "Python/bin/make_otu_table.py"
        self._summarize_taxa_path = "Python/bin/summarize_taxa.py"
        self._sum_tax_path = os.path.join(Config().SOFTWARE_DIR, "meta/scripts/sum_tax.fix.pl")
        self.otu_taxon_dir = os.path.join(self.work_dir, "output", "tax_summary_a")

    def get_biom_otu(self):
        """
        根据in_otu_table和seq_taxon文件生成biom表和otu表

        :return: 生成的biom和otu文件的路径
        """
        otu_tax = dict()
        with open(self.option("taxon_file").prop['path'], 'r') as r1:
            for line in r1:
                line = re.split('\t', line)
                tmp = re.split(';', line[1])
                new_line = '; '.join(tmp)
                otu_tax[line[0]] = new_line
        taxon_otu = os.path.join(self.work_dir, "output", "otu_taxon.xls")
        self.logger.info("正在生成otu_taxon.xls")
        with open(self.option("in_otu_table").prop['path'], 'r') as r2:
            with open(taxon_otu, 'w') as w:
                line1 = r2.next().rstrip('\n')
                if re.search(r'Constructed from biom', line1):
                    line1 = r2.next().rstrip('\n')
                w.write(line1 + "\t" + "taxonomy" + "\n")
                for line in r2:
                    line = line.rstrip('\n')
                    name = re.split('\t', line)[0]
                    line = re.sub(r'\.0', '', line)
                    w.write(line + '\t' + otu_tax[name] + "\n")
        taxon_otu_obj = OtuTableFile()
        taxon_otu_obj.set_path(taxon_otu)
        taxon_otu_obj.get_info()
        new_taxon_otu = taxon_otu + ".new"
        taxon_otu_obj.complete_taxonomy(taxon_otu, new_taxon_otu)
        os.remove(taxon_otu)
        shutil.copy2(new_taxon_otu, taxon_otu)
        os.remove(new_taxon_otu)

        biom = os.path.join(self.work_dir, "output", "otu_taxon.biom")
        cmd = self._biom_path + " convert -i " + taxon_otu + " -o " + biom\
            + " --process-obs-metadata taxonomy --table-type \"OTU table\" --to-hdf5"
        create_taxon_biom = self.add_command("create_taxon_biom", cmd)
        self.logger.info("由otu开始转化biom")
        create_taxon_biom.run()
        self.wait(create_taxon_biom)
        if create_taxon_biom.return_code == 0:
            self.logger.info("taxon_biom生成成功")
        else:
            self.set_error("taxon_biom生成失败")
        return(biom, taxon_otu)

    def get_diff_level(self, biom):
        """
        :param biom: biom文件路径
        """
        tax_summary_a_dir = os.path.join(self.work_dir, "output", "tax_summary_a")
        if os.path.exists(tax_summary_a_dir):
            shutil.rmtree(tax_summary_a_dir)
        cmd = self._summarize_taxa_path + " -i " + biom + ' -o ' + tax_summary_a_dir\
            + " -L 1,2,3,4,5,6,7,8 -a "
        create_tax_summary = self.add_command("create_tax_summary", cmd)
        self.logger.info("开始生成tax_summary_a文件夹")
        create_tax_summary.run()
        self.wait(create_tax_summary)
        if create_tax_summary.return_code == 0:
            self.logger.info("文件夹生成成功")
        else:
            self.logger.info("文件夹生成失败")
        list_ = os.listdir(tax_summary_a_dir)
        for my_otu_table in list_:
            if re.search(r"txt", my_otu_table):
                my_otu_table = os.path.join(tax_summary_a_dir, my_otu_table)
                otu_basename = os.path.basename(my_otu_table)
                otu_basename = re.sub(r'\.txt$', r'.xls', otu_basename)
                otu_name = os.path.join(tax_summary_a_dir, otu_basename)
                cmd = self._sum_tax_path + " -i " + my_otu_table + " -o " + otu_name
            try:
                subprocess.check_call(cmd, shell=True)
            except subprocess.CalledProcessError:
                self.set_error("运行sum_tax.pl出错")
        list_ = os.listdir(tax_summary_a_dir)
        for table in list_:
            if re.search(r"txt$", table):
                file_ = os.path.join(tax_summary_a_dir, table)
                os.remove(file_)
            if re.search(r"new$", table):
                name = re.sub(r"txt\.new$", r"full.xls", table)
                file_ = os.path.join(tax_summary_a_dir, table)
                new_file = os.path.join(tax_summary_a_dir, name)
                os.rename(file_, new_file)
        self.logger.info("开始整理输出文件夹")
        self.rename()

    def rename(self):
        """
        将各级文件重命名，将原始的OTU和biom放入到tax_summary文件夹中
        """
        level_level = {
            "L1": "Domain",
            "L2": "Kingdom",
            "L3": "Phylum",
            "L4": "Class",
            "L5": "Order",
            "L6": "Family",
            "L7": "Genus",
            "L8": "Species"
        }
        tax_summary_a_dir = os.path.join(self.work_dir, "output", "tax_summary_a")
        list_ = os.listdir(tax_summary_a_dir)
        for table in list_:
            match = re.search(r"(.+)(L\d)(.+)", table)
            prefix = match.group(1)
            suffix = match.group(3)
            level = match.group(2)
            newname = prefix + level_level[level] + suffix
            table = os.path.join(tax_summary_a_dir, table)
            newname = os.path.join(tax_summary_a_dir, newname)
            os.rename(table, newname)

        otu_taxon_otu = os.path.join(tax_summary_a_dir, "otu_taxon_otu.xls")
        with open(self.option('in_otu_table').prop['path'], 'r') as r:
            with open(otu_taxon_otu, 'w') as w:
                line1 = r.next()
                if re.search(r'Constructed from biom', line1):
                    line1 = r.next()
                w.write(line1)
                for line in r:
                    line = line
                    line = re.sub(r'\.0', '', line)
                    w.write(line)

        biom = os.path.join(tax_summary_a_dir, "otu_taxon_otu.biom")
        cmd = self._biom_path + " convert -i " + otu_taxon_otu + " -o " + biom\
            + " --table-type \"OTU table\" --to-hdf5"
        create_taxon_biom_otu = self.add_command("create_taxon_otu_biom", cmd)
        self.logger.info("由otu开始转化biom")
        create_taxon_biom_otu.run()
        self.wait(create_taxon_biom_otu)
        if create_taxon_biom_otu.return_code == 0:
            self.logger.info("taxon_biom_otu生成成功")
        else:
            self.set_error("taxon_biom_otu生成失败")

    def run(self):
        """
        运行
        """
        super(OtuTaxonStatTool, self).run()
        (biom, otu_table) = self.get_biom_otu()
        self.option("otu_taxon_biom").set_path(biom)
        self.option("otu_taxon_table").set_path(otu_table)
        self.get_diff_level(biom)
        self.option("otu_taxon_dir").set_path(self.otu_taxon_dir)
        self.logger.info("otu_taxon完成，即将退出程序")
        self.end()
