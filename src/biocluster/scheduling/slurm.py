# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from .job import Job
import os
import gevent
import re
import subprocess


class SLURM(Job):
    """
    openPBS任务调度系统,
    用于生产和管理PBS任务
    """
    def __init__(self, agent):
        super(SLURM, self).__init__(agent)
        self.master_ip = agent.config.JOB_MASTER_IP

    def create_file(self):
        """
        生成PBS脚本用于投递

        :return:
        """
        file_path = os.path.join(self.agent.work_dir, self.agent.name + ".sbatch")
        script = os.path.abspath(os.path.dirname(__file__) + "/../../../bin/runtool.py")
        cpu, mem = self.agent.get_resource()
        if int(cpu) > 32:
            cpu = 32
        with open(file_path, "w") as f:
            f.write("#!/bin/bash\n")
            f.write("#SBATCH -c {}\n".format(cpu))
            f.write("#SBATCH -D %s\n" % self.agent.work_dir)
            f.write("#SBATCH -n 1\n")
            f.write("#SBATCH -N 1\n")
            f.write("#SBATCH -J {}\n".format(self.agent.fullname))
            f.write("#SBATCH -t 10-00:00\n")
            if self.master_ip == "192.168.12.101":
                f.write("#SBATCH -p SANGER\n")
            elif self.master_ip == "192.168.12.102":
                f.write("#SBATCH -p SANGERDEV\n")
            else:
                raise Exception("错误master_ip{}".format(self.master_ip))

            f.write("#SBATCH --mem={}\n".format(mem))
            f.write("#SBATCH -o {}/{}_%j.out\n".format(self.agent.work_dir, self.agent.name))
            f.write("#SBATCH -e {}/{}_%j.err\n".format(self.agent.work_dir, self.agent.name))
            f.write("cd {}\n\n".format(self.agent.work_dir))
            f.write("{} {} {}\n".format("python", script, self.agent.name))

        return file_path

    def submit(self):
        """
        提交PBS任务,并返回Jobid

        :return: jobid
        """
        super(SLURM, self).submit()
        pbs_file = self.create_file()
        cmd = "sbatch {}".format(pbs_file)
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        text = process.communicate()[0]
        if re.match(r'Maximum number', text):
            self.agent.logger.warn("到达最大任务书，30秒后尝试再次投递!")
            gevent.sleep(30)
            self.submit()
        else:
            m = re.search(r'(\d+)$', text)
            if m:
                self.id = m.group(1)
                return self.id
            else:
                self.agent.logger.warn("任务投递系统出现错误:%s，30秒后尝试再次投递!\n" % text)
                gevent.sleep(30)
                self.submit()

    def delete(self):
        """
        删除当前任务

        :return: None
        """

        if self.check_state():
            cmd = "scancel {}".format(self.id)
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
            process.communicate()

    def check_state(self):
        """
        检测任务状态

        :return: string 返回任务状态代码 如果任务不存在 则返回False
        """
        cmd = "scontrol show job {}".format(self.id)
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        text = process.communicate()[0]
        m = re.search(r"JobState = (\w+)", text)
        if m:
            return m.group(1)
        else:
            return False
