# -*- coding: utf-8 -*-
# __author__ = 'shenghe'
from biocluster.agent import Agent
from biocluster.tool import Tool
import os
from biocluster.core.exceptions import OptionError


class DistanceBoxAgent(Agent):
    """
    qiime
    version 1.0
    author shenghe
    last_modified:2015.11.19
    """

    def __init__(self, parent):
        super(DistanceBoxAgent, self).__init__(parent)
        options = [
            {"name": "dis_matrix", "type": "infile",
                "format": "meta.beta_diversity.distance_matrix"},
            {"name": "group", "type": "infile", "format": "meta.otu.group_table"}
        ]
        self.add_option(options)
        self.step.add_steps('distancebox')
        self.on('start', self.step_start)
        self.on('end', self.step_end)

    def step_start(self):
        self.step.distancebox.start()
        self.step.update()

    def step_end(self):
        self.step.distancebox.finish()
        self.step.update()

    def check_options(self):
        """
        重写参数检查
        """
        samplelist = []
        if not self.option('dis_matrix').is_set:
            raise OptionError('必须提供距离矩阵文件')
        else:
            self.option('dis_matrix').get_info()
            samplelist = self.option('dis_matrix').prop['samp_list']
        if not self.option('group').is_set:
            raise OptionError('必须提供分组信息文件')
        else:
            self.option('group').get_info()
            if len(samplelist) != len(self.option('group').prop['sample']):
                raise OptionError('分组文件中样本数量:%s与距离矩阵中的样本数量:%s不一致' % (len(self.option('group').prop['sample']),
                    len(samplelist)))
            for sample in self.option('group').prop['sample']:
                if sample not in samplelist:
                    raise OptionError('分组文件的样本(%s)在otu表的样本中不存在' % sample)
        return True

    def set_resource(self):
        """
        设置所需资源
        """
        self._cpu=2
        self._memory=''


class DistanceBoxTool(Tool):
    def __init__(self, config):
        super(DistanceBoxTool, self).__init__(config)
        self._version='1.9.1'  # qiime版本
        self.cmd_path='Python/bin/make_distance_boxplots.py'
        # self.set_environ(LD_LIBRARY_PATH=self.config.SOFTWARE_DIR + 'gcc/5.1.0/lib64:$LD_LIBRARY_PATH')
        # 设置运行环境变量

    def run(self):
        """
        运行
        """
        super(DistanceBoxTool, self).run()
        self.run_box()

    def add_name(self):
        """
        给一个分组文件添加表头
        """
        groupfile=open(self.option('group').prop['path'], 'r')
        new=open(os.path.join(self.work_dir, 'temp.gup'), 'w')
        new.write('#ID\tgroup\n')
        for i in groupfile:
            new.write(i)
        groupfile.close()
        new.close()

    def linkfile(self, oldfile, newname):
        """
        link文件到output文件夹
        :param oldfile: 资源文件路径
        :param newname: 新的文件名
        :return:
        """
        newpath=os.path.join(self.output_dir, newname)
        if os.path.exists(newpath):
            os.remove(newpath)
        os.link(oldfile, newpath)

    def run_box(self):
        """
        运行qiime:make_distance_boxplots.py
        """
        cmd=self.cmd_path
        self.add_name()
        cmd += ' -m %s -d %s -o %s -f "group" --save_raw_data' % (
            os.path.join(self.work_dir, 'temp.gup'), self.option(
                'dis_matrix').prop['path'],
            self.work_dir)
        self.logger.info('运行qiime:make_distance_boxplots.py程序')
        box_command=self.add_command('box', cmd)
        box_command.run()
        self.wait(box_command)
        if box_command.return_code == 0:
            self.logger.info('运行qiime/make_distance_boxplots.py完成')
            self.linkfile(self.work_dir + '/group_Distances.txt',
                          'group_Distances.txt')
            self.linkfile(self.work_dir + '/group_Stats.txt',
                          'group_Stats.txt')
            self.end()
        else:
            self.set_error('运行qiime/make_distance_boxplots.py出错')
