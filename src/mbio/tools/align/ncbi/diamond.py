# -*- coding: utf-8 -*-
# __author__ = 'Shijin'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError


class DiamondAgent(Agent):
    """
    diamond version: 0.8.35
    version 1.0
    author: shijin
    last_modify: 20170316
    """
    def __init__(self, parent):
        super(DiamondAgent, self).__init__(parent)
        options = [
            {"name": "query", "type": "infile", "format": "sequence.fasta"},  # 输入文件
            {"name": "query_type", "type": "string", "default": "nucl"},  # 输入的查询序列的格式，为nucl或者prot
            {"name": "database", "type": "string", "default": "plant"},
            # 比对数据库 plant, nr, etc.
            {"name": "outfmt", "type": "int", "default": 6},  # 输出格式，数字遵从blast+
            {"name": "blast", "type": "string", "default": "blastp"},  # 设定diamond程序有blastp，blastx
            {"name": "reference", "type": "infile", "format": "sequence.fasta"},  # 参考序列  选择customer时启用
            {"name": "evalue", "type": "float", "default": 1e-5},  # evalue值
            {"name": "num_threads", "type": "int", "default": 10}  # cpu数
            ]
        self.add_option(options)
        self.step.add_steps('diamond')
        self.on('start', self.step_start)
        self.on('end', self.step_end)
        self.queue = 'BLAST'  # 投递到指定的队列BLAST

    def step_start(self):
        self.step.diamond.start()
        self.step.update()

    def step_end(self):
        self.step.diamond.finish()
        self.step.update()

    def check_options(self):
        if not self.option("query").is_set:
            raise OptionError("必须设置参数query")
        if self.option('query_type') not in ['nucl', 'prot']:
            raise OptionError('query_type查询序列的类型为nucl(核酸)或者prot(蛋白):{}'.format(self.option('query_type')))
        if not 1 > self.option('evalue') >= 0:
            raise OptionError('E-value值设定必须为[0-1)之间：{}'.format(self.option('evalue')))

        return True

    def set_resource(self):
        self._cpu = self.option('num_threads')
        self._memory = '20G'

    def end(self):
        super(DiamondAgent, self).end()


class DiamondTool(Tool):
    def __init__(self, config):
        super(DiamondTool, self).__init__(config)
        self._version = "0.8.35"
        self.db_path = os.path.join(self.config.SOFTWARE_DIR, "database/align/diamond")
        self.cmd_path = "bioinfo/align/diamond-0.8.35"   # 执行程序路径必须相对于 self.config.SOFTWARE_DIR
        if self.option("query_type") == "nucl":
            self.blast_type = "blastx"
        else:
            self.blast_type = "blastp"

    def run_makedb_and_diamond(self):
        """
        创建diamond数据库并运行diamond

        :return:
        """
        db_name = os.path.splitext(os.path.basename(self.option("reference").prop['path']))[0]
        cmd = os.path.join(self.cmd_path, "makedb")
        self.db_path = os.path.join(self.work_dir, 'diamond')
        cmd += "-makedb -in {} -d {}".format(self.option("reference").prop['path'], db_name)
        self.logger.info("开始创建diamond数据库，生成结果库文件放在工作目录的customer_blastdb下")
        makedb_obj = self.add_command("makedb", cmd).run()
        self.wait(makedb_obj)
        if makedb_obj.return_code == 0:
            self.logger.info("创建diamond数据库完成")
            self.run_diamond(db_name)
        else:
            self.set_error("创建diamond数据库出错!")

    def run_diamond(self, db_name):
        """
        运行diaomond

        :param db_name: blastdb名称
        :return:
        """
        db = os.path.join(self.db_path, db_name)
        query_name = os.path.splitext(os.path.basename(self.option("query").prop['path']))[0]
        cmd = os.path.join(self.cmd_path, "diamond")
        outputfile = os.path.join(self.output_dir, query_name + "_vs_" + db_name)
        outfmt = self.option('outfmt')
        if self.option('outfmt') == 5:
            outputfile += '.xml'
            outfmt = 5
        elif self.option('outfmt') == 6:
            outputfile += '.txt'
            outfmt = 6  # 为了保证table格式输出表头完全一致，输出为6时，选用5xml为输出结果，后面再通过统一的xml2table转换
        else:
            pass
        cmd += " {} -q {} -d {} -o {} -e {} -f {} -p {}".format(
            self.blast_type, self.option("query").prop['path'], db, outputfile,
            self.option("evalue"), outfmt, self.option("num_threads"))
        self.logger.info("开始运行blast")
        blast_command = self.add_command("diamond", cmd)
        blast_command.run()
        self.wait()
        if blast_command.return_code == 0:
            self.logger.info("运行diamond完成")
            if self.option('outfmt') == 6:
                pass
            if self.option('outfmt') == 5:
                self.logger.info(outputfile)
                # self.option('outxml', outputfile)
            self.end()
        else:
            self.set_error("diamond运行出错!")

    def run(self):
        """
        运行
        :return:
        """
        super(DiamondTool, self).run()
        if self.option("database") == 'customer_mode':
            self.run_makedb_and_diamond()
        else:
            self.run_diamond(self.option("database"))
