# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

from .job import Job
import os
import re
import gevent


class LOCAL(Job):
    def __init__(self, agent):
        super(LOCAL, self).__init__(agent)

    def submit(self):
        script = os.path.abspath(os.path.dirname(__file__) + "/../../../bin/runtool.py")
        cmd = 'cd %s;%s -b %s;sleep 1;cat run.pid' % (self.agent.work_dir, script, self.agent.name)
        output = os.popen(cmd)
        text = output.read()
        m = re.search(r'(\d+)', text)
        if m:
            self.id = m.group(1)
            return self.id
        else:
            self.agent.logger.warn("任务投递出现错误:%s，30秒后尝试再次投递!\n" % output)
            gevent.sleep(30)
            self.submit()

    def delete(self):
        if self.check_state():
            os.system('kill -9 %s"' % self.id)

    def check_state(self):
        output = os.popen('ps -q %s -f --no-heading' % self.id)
        text = output.read()
        if re.search(r"runtool", text):
            return True
        else:
            return False
