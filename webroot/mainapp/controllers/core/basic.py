# -*- coding: utf-8 -*-
# __author__ = 'guoquan'

import traceback
from biocluster.wpm.client import worker_client, wait


class Basic(object):
    def __init__(self, data, instant=False):
        self._instant = instant
        self._json = data
        self._id = data["id"]
        self._return_msg = None

    @property
    def id(self):
        """
        获取运行任务的ID

        :return:
        """
        return self._id

    @property
    def instant(self):
        """
        任务是否是即时计算

        :return: bool
        """
        return self._instant

    @property
    def return_msg(self):
        """
        获取运行任务的返回值

        :return:
        """
        return self._return_msg

    def run(self):
        try:
            worker = worker_client()
            info = worker.add_task(self._json)
            # self.logger.info("infor: %s"%(info))
            if info["success"]:
                if self.instant:
                    return self.instant_wait(worker)
                else:
                    return info
            else:
                return False
        except Exception, e:
            exstr = traceback.format_exc()
            print "ERROR:", exstr
            raise Exception("任务提交失败：%s, %s" % (str(e), str(exstr)))

    def instant_wait(self, worker):
        end = wait(self._id)
        if end is True:
            self._return_msg = worker.get_msg(self._id)
            return self._return_msg
        else:
            raise Exception("运行超时!")
