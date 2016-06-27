# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
from biocluster.core.exceptions import OptionError
import threading
import re


class GiToTaxonAgent(Agent):
    """
    gi2taxon.py v1.0
    author: shenghe
    last_modify: 2016.6.21
    """

    def __init__(self, parent):
        super(GiToTaxonAgent, self).__init__(parent)
        options = [
            {"name": "blastout", "type": "infile", "format": "align.blast.blast_xml, align.blast.blast_table"},  # 输入文件
            {"name": "blastdb", 'type': 'string', 'default': 'None'}  # 输入文件的blast比对类型，必须为nr或者nt
            ]
        self.add_option(options)

    def check_options(self):
        if not self.option("blastout").is_set:
            raise OptionError("必须设置参数blastout")
        if self.option('blastdb') == 'None':
            raise OptionError("blastdb参数必须设置")
        else:
            if self.option('blastdb') not in ['nr', 'nt']:
                raise OptionError('blastdb必须为')
        return True

    def set_resource(self):
        self._cpu = 10
        self._memory = ''

    def end(self):
        result_dir = self.add_upload_dir(self.output_dir)
        result_dir.add_relpath_rules([
            [".", "", "结果输出目录"],
            ['query_taxons_detail.xls', 'xls', '序列详细物种分类文件']
            ])
        super(GiToTaxonAgent, self).end()


class GiToTaxonTool(Tool):
    def __init__(self, config):
        super(GiToTaxonTool, self).__init__(config)
        self._version = "1.0"

    class result_thread(threading.Thread):
        def __init__(self, func, *argu, **kwargu):
            super(GiToTaxonTool.result_thread, self).__init__()
            self.func = func
            self.argu = argu
            self.kwargu = kwargu
            self.result = None

        def run(self):
            self.result = self.func(*self.argu, **self.kwargu)


    def run(self):
        """
        运行
        :return:
        """
        super(GiToTaxonTool, self).run()
        if self.option("blastout").format == 'align.blast.blast_xml':
            from mbio.packages.align.blast.xml2table import xml2table
            table = xml2table(self.option('blastout').path, self.work_dir + '/temp_blastable.xls')
        else:
            table = self.option('blastout').path
        if self.option('blastdb') == 'nr':
            db = 'prot'
        else:
            db = 'nucl'
        run_gitaxon = self.result_thread(self.process_run, table, db)
        run_gitaxon.start()
        run_gitaxon.join()
        if run_gitaxon.result:
            self.end()
        else:
            self.set_error('注释查询出错！')


    def process_run(self, fp, db):
        """单独的线程运行查询分类注释"""
        from mbio.packages.taxon.gi2taxon import gi_taxon  # import进入了sqlite3的对象，这个对象不可以跨线程使用
        query_gi = self.filter_query(fp)
        gi_taxons = gi_taxon(set(query_gi.values()), db)
        for i in query_gi:
            query_gi[i] = (query_gi[i], gi_taxons[query_gi[i]])
        with open(self.output_dir + '/query_taxons_detail.xls', 'w') as w:
            for item in query_gi.iteritems():
                if item[1][1]:
                    w.write(item[0] + '\t' + item[1][0] + item[1][1] + '\n')
        return True

    def filter_query(self, fp):
        """生成query对gi的"""
        query_gi = dict()
        openfp = open(fp)
        openfp.readline()
        for i in openfp:
            line_sp = i.split('\t')
            if line_sp[5] in query_gi:
                continue
            else:
                hitname = line_sp[10].split('|')
                if len(hitname) < 3 or hitname[0] != 'gi' or (not re.match(r'^\d+$', hitname[1])):
                    self.set_error('输入文件中不是nr库比对结果(20160622)不含有gi信息:{}'.format(line_sp[10]))
                query_gi[line_sp[5]] = hitname[1]
        return query_gi
