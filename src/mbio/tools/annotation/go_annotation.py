# -*- coding: utf-8 -*-
# __author__ = 'wangbixuan'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.config import Config
import os
from biocluster.core.exceptions import OptionError
# import xml.etree.ElementTree as ET
import subprocess


class GoAnnotationAgent(Agent):
    """
    to perform Gene Ontology Annotation
    author: wangbixuan
    last_modified: 20160728
    """

    def __init__(self, parent):
        super(GoAnnotationAgent, self).__init__(parent)
        options = [
            {"name": "blastout", "type": "infile",
                "format": "align.blast.blast_xml"},
            {"name": "go2level_out", "type": "outfile",
                "format": "annotation.go.level2"},
            {"name": "golist_out", "type": "outfile",
                "format": "annotation.go.go_list"}
        ]
        self.add_option(options)
        self.step.add_steps('go_annotation')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.go_annotation.start()
        self.step.update()

    def step_end(self):
        self.step.go_annotation.finish()
        self.step.update()

    def check_options(self):
        if self.option("blastout").is_set:
            '''
            document = ET.parse(self.option("blastout").prop['path'])
            root = document.getroot()
            db = root.find('BlastOutput_db')
            if db.text == 'nr':
                pass
            else:
                raise OptionError("BLAST比对数据库不支持")
            '''
            pass
        else:
            raise OptionError("必须提供BLAST结果文件")

    def set_resource(self):
        self._cpu = 10
        self._memory = '15G'

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
        ])
        result_dir.add_regexp_rules([
            ["./blast2go.annot", "annot", "Go annotation based on blast output"],
            ["./query_gos.list", "list", "Merged Go annotation"],
            ["./go1234level_statistics.xls", "xls", "Go annotation on 4 levels"],
            ["./go2level.xls", "xls", "Go annotation on level 2"],
            ["./go3level.xls", "xls", "Go annotation on level 3"],
            ["./go4level.xls", "xls", "Go annotation on level 4"]
        ])
        super(GoAnnotationAgent, self).end()


class GoAnnotationTool(Tool):

    def __init__(self, config):
        super(GoAnnotationTool, self).__init__(config)
        self._version = "1.0"  # to be changed
        # self.cmd_path

    def run(self):
        super(GoAnnotationTool, self).run()
        self.run_b2g()
        # self.run_gomerge()
        # self.run_annotation()
        # self.run_gosplit()

    def run_b2g(self):
        cmd = "java -Xmx500m -cp /mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/b2g4pipe_v2.5/*:/mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/b2g4pipe_v2.5/ext/*: es.blast2go.prog.B2GAnnotPipe -in %s -prop /mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/b2g4pipe_v2.5/b2gPipe.properties -annot -out %s" % (self.option("blastout").prop[
                                                                                                                                                                                                                                                                                                                                    'path'], self.work_dir + '/blast2go')
        #cmd="java -Xms128m -Xmx80000m -cp /mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/b2g4pipe_v2.5/*:ext/*: es.blast2go.prog.B2GAnnotPipe –in %s -prop /mnt/ilustre/users/sanger-dev/app/bioinfo/annotation/b2gPipe.properties -out %s -annot"%(self.option("blastout").prop['path'],self.work_dir+'/blast2go')
        self.logger.info('运行b2g程序')
        self.logger.info(cmd)
        try:
            subprocess.check_output(cmd, shell=True)
            self.logger.info('运行b2g完成')
            linkfile = self.output_dir + '/blast2go.annot'
            if os.path.exists(linkfile):
                os.remove(linkfile)
            os.link(self.work_dir + '/blast2go.annot', linkfile)
            # self.run_gomerge
            # self.end()
        except subprocess.CalledProcessError as e:
            self.logger.debug(e)
            self.set_error('运行b2g出错')
        self.logger.debug("b2g end")
        self.run_gomerge()

    def run_gomerge(self):
        cmd1 = '{}/program/Python/bin/python {}/bioinfo/annotation/scripts/goMerge.py'.format(
            self.config.SOFTWARE_DIR, self.config.SOFTWARE_DIR)
        cmd1 += ' %s %s' % (
            self.work_dir + '/blast2go.annot', 'GO.list')
        # Config.DB_HOST,Config.DB_USER,Config.DB_PASSWD
        self.logger.info("运行mergeGO.py")
        self.logger.info(cmd1)

        # gomerge_command=self.add_command('gomerge',cmd1)
        # gomerge_command.run()
        # self.wait()

        # if gomerge_command.return_code==0:
        # self.logger.info("运行mergeGO.py完成")
        try:
            subprocess.check_output(cmd1, shell=True)
            if os.path.exists(self.output_dir + '/GO.list'):
                os.remove(self.output_dir + '/GO.list')
            if os.path.exists(self.output_dir + '/query_gos.list'):
                os.remove(self.output_dir + '/query_gos.list')
            os.link(self.work_dir + '/GO.list',
                    self.output_dir + '/query_gos.list')
            self.option('golist_out', self.output_dir + '/query_gos.list')
            # self.run_annotation
        # else:
        except subprocess.CalledProcessError:
            self.set_error('运行mergeGO.py出错')
        self.run_annotation()

    def run_annotation(self):
        cmd2 = '{}/program/Python/bin/python {}/bioinfo/annotation/scripts/goAnnot.py'.format(
            self.config.SOFTWARE_DIR, self.config.SOFTWARE_DIR)
        cmd2 += ' %s %s %s %s' % (
            self.work_dir + '/GO.list', '10.100.203.193', Config().DB_USER, Config().DB_PASSWD)#10.100.203.193
        self.logger.info("运行goAnnot.py")
        self.logger.info(cmd2)
        '''
        annotation_command=self.add_command('annotation',cmd2)
        annotation_command.run()
        self.wait()
        if annotation_command.return_code==0:
        '''
        try:
            subprocess.check_output(cmd2, shell=True)
            self.logger.info("运行goAnnot.py完成")
            if os.path.exists(self.output_dir + '/go1234level_statistics.xls'):
                os.remove(self.output_dir + '/go1234level_statistics.xls')
            os.link(self.work_dir + '/go1234level_statistics.xls',
                    self.output_dir + '/go1234level_statistics.xls')
            # self.end()
            # self.run_gosplit
        except subprocess.CalledProcessError:
            self.set_error("运行goAnnot.py出错")
        self.run_gosplit()

    def run_gosplit(self):
        cmd3 = '{}/program/Python/bin/python {}/bioinfo/annotation/scripts/goSplit.py'.format(
            self.config.SOFTWARE_DIR, self.config.SOFTWARE_DIR)
        cmd3 += ' %s' % self.work_dir + '/go_detail.xls'
        self.logger.info("运行goSplit.py")
        self.logger.info(cmd3)
        '''
        split_command=self.add_command('split',cmd3)
        split_command.run()
        self.wait()
        if split_command.return_code==0:
        '''
        try:
            subprocess.check_output(cmd3, shell=True)
            self.logger.info("运行goSplit.py完成")
            outfiles = ['go2level.xls', 'go3level.xls', 'go4level.xls']
            # linkfile
            for item in outfiles:
                linkfile = self.output_dir + '/' + item
                if os.path.exists(linkfile):
                    os.remove(linkfile)
                os.link(self.work_dir + '/' + item, linkfile)
            self.option('go2level_out', self.output_dir + '/go2level.xls')
        except subprocess.CalledProcessError:
            self.set_error("运行goSplit.py出错")
        self.end()
