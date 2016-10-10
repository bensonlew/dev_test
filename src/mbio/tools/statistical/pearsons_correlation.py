# -*- coding: utf-8 -*-
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import subprocess
import os


class PearsonsCorrelationAgent(Agent):
    """
    pearsonsCorrelation:用于生成环境因子和otu/taxon之间的correlation
    version: 0.1
    author: wangbixuan
    last_modified: 20160930 by qindanhua
    """
    def __init__(self, parent):
        super(PearsonsCorrelationAgent, self).__init__(parent)
        options = [
            {"name": "otutable", "type": "infile", "format": "meta.otu.otu_table, meta.otu.tax_summary_dir"},
            {"name": "level", "type": "string", "default": "otu"},
            {"name": "envtable", "type": "infile", "format": "meta.otu.group_table"},
            {"name": "envlabs", "type": "string", "default": ""},
            {"name": "cor_table", "type": "outfile", "format": "meta.otu.group_table"},
            {"name": "pvalue_table", "type": "outfile", "format": "meta.otu.group_table"}
        ]
        self.add_option(options)
        self.step.add_steps('pearsons_correlation')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.pearsons_correlation.start()
        self.step.update()

    def step_end(self):
        self.step.pearsons_correlation.finish()
        self.step.update()

    def gettable(self):
        """
        根据level返回进行计算的矩阵
        """
        if self.option("otutable").format == "meta.otu.tax_summary_dir":
            return self.option("otutable").get_table(self.option('level'))
        else:
            return self.option('otutable').prop['path']

    def check_options(self):
        if self.option("level") not in ['otu', 'domain', 'kindom', 'phylum', 'class', 'order',
                                        'family', 'genus', 'species']:
            raise OptionError("请选择正确的分类水平")
        if not self.option("otutable").is_set:
            raise OptionError('必须提供otu表')
        self.option('otutable').get_info()
        if self.option('envtable').is_set:
            self.option('envtable').get_info()
            if self.option('envlabs'):
                labs = self.option('envlabs').split(',')
                for lab in labs:
                    if lab not in self.option('envtable').prop['group_scheme']:
                        raise OptionError('该envlabs中的因子不存在于环境因子表：%s' % lab)
            else:
                pass
        else:
            raise OptionError('请选择环境因子表')

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu = 2
        self._memory = '2G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "PearsonsCorrelation计算结果输出目录"],
            ["./pearsons_correlation_at_'%s'_level.xls" % self.option('level'), "xls", "PearsonsCorrelation矩阵"],
            ["./pearsons_pvalue_at_'%s'_level.xls" % self.option('level'), "xls", "PearsonsCorrelationPvalues"]
            ])
        super(PearsonsCorrelationAgent, self).end()


class PearsonsCorrelationTool(Tool):
    def __init__(self, config):
        super(PearsonsCorrelationTool, self).__init__(config)
        self.perl_path = self.config.SOFTWARE_DIR + "/program/perl/perls/perl-5.24.0/bin/"
        self.hcluster_script_path = self.config.SOFTWARE_DIR + "/bioinfo/statistical/scripts/"
        self.Rscript_path = self.config.SOFTWARE_DIR + "/program/R-3.3.1/bin/"
        self.cmd_path = '{}/program/Python/bin/python {}/bioinfo/statistical/scripts/pearsonsCorrelation.py'.format(self.config.SOFTWARE_DIR, self.config.SOFTWARE_DIR)
        # self.cmd_path=os.path.join(self.config.SOFTWARE_DIR, 'bioinfo/statistical/scripts/pearsonsCorrelation.py')
        self.env_table = self.get_new_env()
        self.real_otu = self.get_otu_table()

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

    def get_new_env(self):
        """
        根据envlabs生成新的envtable
        """
        if self.option('envlabs'):
            new_path = self.work_dir + 'temp_env_table.xls'
            self.option('envtable').sub_group(new_path, self.option('envlabs').split(','))
            return new_path
        else:
            return self.option('envtable').prop['path']

    def run(self):
        """
        运行
        """
        super(PearsonsCorrelationTool, self).run()
        self.run_pearsonsCorrelation()
        # self.plot_hcluster()
        self.set_output()
        self.end()

    def run_pearsonsCorrelation(self):
        """
        run pearsonsCorrelation.py
        """
        cmd = self.cmd_path
        cmd += " %s %s %s %s" % (self.real_otu, self.env_table, "./pearsons_correlation_at_'%s'_level.xls" % self.option('level'), "./pearsons_pvalue_at_'%s'_level.xls" % self.option('level'))
        self.logger.info('运行pearsonsCorrelation.py计算correlation')
        self.logger.info(cmd)
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info('Pearsons Correlation 计算成功')
        except subprocess.CalledProcessError:
            self.logger.info('Pearsons Correlation 计算失败')
            self.set_error('pearsonsCorrelation.py 计算失败')
        self.logger.info('运行pearsonsCorrelation.py计算correlation完成')
        # self.set_output()
        # self.end()

    # def plot_hcluster(self):
    #     perl_cmd = "{}perl {}plot-hcluster_tree.pl -i {} -o {}".format(self.perl_path, self.hcluster_script_path,
    #                                                                    "./pearsons_correlation_at_'%s'_level.xls" % self.option('level'), "hcluster")
    #     r_cmd = "{}Rscript {}".format(self.Rscript_path, "hc.cmd.r")
    #     self.logger.info(perl_cmd)
    #     self.logger.info(r_cmd)
    #     os.system(perl_cmd)
    #     try:
    #         subprocess.check_output(r_cmd, shell=True)
    #         self.logger.info("OK")
    #         return True
    #     except subprocess.CalledProcessError:
    #         self.logger.info("运行hcluster出错")
    #         return False

    def set_output(self):
        newpath = self.output_dir + "/pearsons_correlation_at_%s_level.xls" % self.option('level')
        if os.path.exists(newpath):
            os.remove(newpath)
        os.link(self.work_dir + "/pearsons_correlation_at_%s_level.xls" % self.option('level'),
                self.output_dir + "/pearsons_correlation_at_%s_level.xls" % self.option('level'))
        self.option('cor_table', newpath)

        newpath2 = self.output_dir + "/pearsons_pvalue_at_%s_level.xls" % self.option('level')
        if os.path.exists(newpath2):
            os.remove(newpath2)
        os.link(self.work_dir + "/pearsons_pvalue_at_%s_level.xls" % self.option('level'),
                self.output_dir + "/pearsons_pvalue_at_%s_level.xls" % self.option('level'))
        self.option('pvalue_table', newpath2)
