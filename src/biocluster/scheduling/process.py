# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from .job import Job
import os
import gevent
import re
from multiprocessing import Process, Manager
import pickle
from biocluster.agent import PickleConfig
import os
from biocluster.core.function import load_class_by_path,  get_classpath_by_object


class PROCESS(Job):
    """
    用于本地直接运行
    """
    def __init__(self, agent):
        super(PROCESS, self).__init__(agent)
        shared_callback_action = Manager().dict()
        agent.shared_callback_action = shared_callback_action
        workflow = agent.get_workflow()
        self.process = LocalProcess(agent, workflow.rpc_server.process_queue, shared_callback_action)

    def submit(self):
        super(PROCESS, self).submit()
        self.process.start()

    def delete(self):
        self.process.terminate()

    def set_end(self):
        super(PROCESS, self).set_end()
        self.process.join()


class LocalProcess(Process):
    def __init__(self, agent, shared_queue, shared_callback_action):
        super(LocalProcess, self).__init__()
        self.agent = agent
        self._shared_queue = shared_queue
        self._shared_callback_action = shared_callback_action
        
    def run(self):
        super(LocalProcess, self).run()
        os.chdir(self.agent.work_dir)

        file_class_paths = []  #
        for option in self.agent.get_option_object().values():
            if option.type in {'outfile', 'infile'}:
                if option.format:
                    file_class_paths.append(option.format)
                else:
                    for f in option.format_list:
                        file_class_paths.append(f)
        for file_class in file_class_paths:
            load_class_by_path(file_class, "File")

        tool_path = get_classpath_by_object(self.agent)
        paths = tool_path.split(".")
        paths.pop(0)
        paths.pop(0)
        tool = load_class_by_path(".".join(paths), "Tool")

        config = PickleConfig()
        config.clone(self.agent)
        config.DEBUG = False
        tool.shared_callback_action = self._shared_callback_action
        tool.shared_queue = self._shared_queue
        config._instant = True
        tool(config).run()
