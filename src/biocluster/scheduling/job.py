# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from ..core.singleton import singleton
from ..config import Config
import gevent
import importlib
import datetime
from ..core.watcher import Watcher
import copy


@singleton
class JobManager(object):
    """
    管理单个流程中的所有Job对象::

        singleton单例类，只会实例化一次
    """
    def __init__(self):
        config = Config()
        self.run_jobs = []
        self.default_mode = config.JOB_PLATFORM
        self.max_job_number = config.MAX_JOB_NUMBER
        self.queue_jobs = []
        Watcher().add(self._watch_waiting_jobs, 10)
        Watcher().add(self._watch_processing_jobs, 10)

    def add_job(self, agent):
        """
        根据agent信息生成一个Job子类对象，并且加入job队列开始运行

        :return:
        """
        mode = self.default_mode.lower()
        if agent.mode.lower() != "auto":
            mode = agent.mode.lower()
        module = importlib.import_module("biocluster.scheduling.%s" % mode)
        job = getattr(module, mode.upper())(agent)

        if len(self.get_unfinish_jobs()) >= self.max_job_number:
            self.queue_jobs.append(job)
            agent.logger.info("任务队列达到最大上限%s个，排队等待运行!" % self.max_job_number)
            agent.is_wait = True
        else:
            agent.is_wait = False
            agent.logger.info("开始投递远程任务!")
            self.run_jobs.append(job)
            if job.submit():
                agent.logger.info("任务投递成功,任务类型%s , ID: %s!" % (mode, job.id))
                agent._start_queue_time = datetime.datetime.now()
            else:
                agent.logger.error("任务投递失败!")
                agent.get_workflow().exit(data="Tool %s 任务投递失败!" % agent.id)
        return job

    def get_all_jobs(self):
        """
        获取所有任务对象

        :return: list  Job子类对象列表
        """
        jobs = copy.copy(self.run_jobs)
        jobs.extend(self.queue_jobs)
        return jobs

    def get_unfinish_jobs(self):
        """
        获取未完成的任务对象

        :return: list  Job子类对象列表
        """
        un_done = []
        for job in self.run_jobs:
            if not job.is_end:
                un_done.append(job)
        return un_done

    def get_job(self, agent):
        """
        根据agent对象找出其对应的Job子类，如果没有找到，则返回False

         :return:  Job子类对象
        """
        jobs = copy.copy(self.run_jobs)
        jobs.extend(self.queue_jobs)
        for job in jobs:
            if agent is job.agent:
                return job
        return False

    def _watch_waiting_jobs(self):
        """
        监控等待排队的任务，满足任务运行条件时开始运行任务

        :return:
        """
        # while True:
        #     gevent.sleep(30)
        jobs = copy.copy(self.queue_jobs)
        for queue_job in jobs:
            if len(self.get_unfinish_jobs()) < self.max_job_number:
                queue_job.agent.is_wait = False
                queue_job.agent.logger.info("开始投递任务!")
                self.run_jobs.append(queue_job)
                mode = self.default_mode.lower()
                if queue_job.agent.mode.lower() != "auto":
                    mode = queue_job.agent.mode.lower()
                if queue_job.submit():
                    queue_job.agent.logger.info("任务投递成功,任务类型%s , ID: %s!" % (mode, queue_job.id))
                    self.queue_jobs.remove(queue_job)
                    queue_job.agent._start_queue_time = datetime.datetime.now()
                else:
                    queue_job.agent.logger.error("任务投递失败!")
                    queue_job.agent.get_workflow().exit(data="Tool %s 任务投递失败!" % queue_job.agent.id)

    def _watch_processing_jobs(self):
        """
        监视本地进程模式时进程意外终止的问题

        :return:
        """
        if len(self.get_all_jobs()) == 0:
            return
        is_process = False
        for running_job in self.get_unfinish_jobs():
            if hasattr(running_job, "process"):
                is_process = True
                if not running_job.process.is_alive():
                    gevent.sleep(10)
                    if not running_job.is_end:
                        running_job.agent.fire("error", "任务%s进程意外结束，请查看运行日志!" % running_job.agent.id)

        if is_process is False:
            return "exit"

    def remove_all_jobs(self):
        """
        删除所有未完成任务

        :return:
        """
        for job in self.run_jobs:
            if not job.is_end:
                job.delete()


class Job(object):
    """
    Job基类,
    用于扩展各种集群调度平台
    """
    def __init__(self, agent):
        self.agent = agent
        self.id = 0
        self._end = False
        self.submit_time = None
        self.state = None

    @property
    def is_end(self):
        """
        返回是否已经完成

        :return: bool
        """
        return self._end

    def submit(self):
        """
        提交任务,需在子类中重写

        :return:
        """
        self.submit_time = datetime.datetime.now()

    def resubmit(self):
        """
        删除任务并重新提交

        :return: None
        """
        self.delete()
        self.id = 0
        self.submit_time = None
        self._end = False
        self.submit()

    def delete(self):
        """
        删除任务,需要在子类中重写此方法

        :return:
        """
        pass

    def check_state(self):
        """
        检测任务状态,需要在子类中重写此方法

        :return: string 返回任务状态代码 如果任务不存在 则返回False
        """
        pass

    def set_end(self):
        """
        将Job状态设置为已完成
        :return:
        """
        self._end = True

    def is_queue(self):
        """
        判断是否正在排队
        :return:
        """
        pass

    def is_running(self):
        """
        判断是否正在运行
        :return:
        """
        pass

    def is_error(self):
        """
        判断是否出现错误
        :return:
        """
        pass

    def is_completed(self):
        """
        判断任务是否完成
        :return:
        """
        pass
