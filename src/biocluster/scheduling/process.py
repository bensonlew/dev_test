# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from .job import Job
import os
import gevent
import re
from multiprocessing import Manager
import pickle
from biocluster.agent import PickleConfig
import os
from biocluster.core.function import load_class_by_path,  get_classpath_by_object
# from gipc.gipc import _GProcess as Process
from biocluster.logger import Wlog
import gipc


class PROCESS(Job):
    """
    用于本地直接运行
    """
    def __init__(self, agent):
        super(PROCESS, self).__init__(agent)
        self.agent = agent
        self.workflow = agent.get_workflow()
        if not hasattr(self.workflow, "process_share_manager"):
            self.workflow.process_share_manager = Manager()
        self.shared_callback_action = self.workflow.process_share_manager.dict()
        agent.shared_callback_action = self.shared_callback_action

        self.process = None

    def submit(self):
        super(PROCESS, self).submit()
        # self.process.start()
        self.process = gipc.start_process(local_process_run, args=(self.agent,
                                                                   self.workflow.rpc_server.process_queue,
                                                                   self.shared_callback_action,), daemon=True)
        self.id = self.process.pid

    def delete(self):
            if self.process.is_alive():
                self.process.terminate()
                # self.manager.shutdown()
            else:
                self.process.join()
                # self.manager.shutdown()

    def set_end(self):
        super(PROCESS, self).set_end()
        self.delete()


def local_process_run(agent, process_queue, shared_callback_action):
    #     # super(LocalProcess, self).__init__()
    #     self.agent = agent
    #     self._process_pipe_writer = process_pipe_writer
    #     self._shared_callback_action = shared_callback_action
    #
    # def run(self):
        # super(LocalProcess, self).run()
        agent.get_workflow().rpc_server.close()
        # Watcher().stopall()
        gevent.sleep(0)
        os.chdir(agent.work_dir)
        file_class_paths = []  #
        for option in agent.get_option_object().values():
            if option.type in {'outfile', 'infile'}:
                if option.format:
                    file_class_paths.append(option.format)
                else:
                    for f in option.format_list:
                        file_class_paths.append(f)
        for file_class in file_class_paths:
            load_class_by_path(file_class, "File")

        tool_path = get_classpath_by_object(agent)
        paths = tool_path.split(".")
        paths.pop(0)
        paths.pop(0)
        tool = load_class_by_path(".".join(paths), "Tool")

        config = PickleConfig()
        config.clone(agent)
        config.DEBUG = False
        config.instant = True
        itool = tool(config)
        itool.logger = Wlog(itool).get_logger('Tool子进程 %s (parent: %s )' % (os.getpid(), os.getppid()))
        itool.shared_callback_action = shared_callback_action
        itool.process_queue = process_queue
        itool.run()
