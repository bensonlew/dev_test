# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from gevent import monkey; monkey.patch_all()
from .job import Job
import os
import re
import gevent


class SSH(Job):
    def __init__(self, agent):
        super(SSH, self).__init__(agent)
        self.server_ip = agent.config.SSH_DEFAULT_IP

    def set_server_ip(self, ip):
        self.server_ip = ip

    def submit(self):
        script = os.path.abspath(os.path.dirname(__file__) + "/../../../bin/runtool.py")
        cmd = 'ssh -o GSSAPIAuthentication=no %s "cd %s;%s -b %s;sleep 1;cat run.pid"' \
              % (self.server_ip, self.agent.work_dir, script, self.agent.name)
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
            os.system('ssh -o GSSAPIAuthentication=no %s "kill -9 %s"' % (self.server_ip, self.id))

    def check_state(self):
        output = os.popen('ssh -o GSSAPIAuthentication=no %s "ps -q %s -f --no-heading"' % (self.server_ip, self.id))
        text = output.read()
        if re.search(r"runtool", text):
            return True
        else:
            return False
