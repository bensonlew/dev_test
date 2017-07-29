# -*- coding: utf-8 -*-
# __author__ = 'xuting'

import logging
from .config import Config
from .core.singleton import singleton


@singleton
class Wlog(object):
    """
    日志类
    """

    def __init__(self, workflow=None):
        """
        """
        config = Config()
        log_level = {'debug': logging.DEBUG,
                     'info': logging.INFO,
                     'warning': logging.WARNING,
                     'error': logging.ERROR,
                     'critical': logging.CRITICAL}
        self.format = config.LOG_FORMAT
        self.formatter = logging.Formatter(self.format, "%Y-%m-%d %H:%M:%S")
        self.level = log_level[config.LOG_LEVEL.lower()]
        self.streem_on = config.LOG_STREEM
        self.workflow = workflow
        if workflow:
            self.log_path = workflow.work_dir + "/log.txt"
            self.file_handler = logging.FileHandler(self.log_path)
            self.file_handler.setLevel(self.level)
            self.file_handler.setFormatter(self.formatter)
        if self.streem_on:
            self.stream_handler = logging.StreamHandler()
            self.stream_handler.setLevel(self.level)
            self.stream_handler.setFormatter(self.formatter)
        self.logger = None

    def __del__(self):
        self.destroy()

    def destroy(self):
        if self.stream_handler and self.logger:
            self.logger.removeHandler(self.stream_handler)
        if self.file_handler and self.logger:
            self.logger.removeHandler(self.file_handler)

    def get_logger(self, name=""):
        """
        返回一个logger对象

        :param name: logger名字
        """
        self.logger = logging.getLogger(name)
        self.logger.propagate = 0
        self._add_handler(self.logger)
        return self.logger

    def _add_handler(self, logger):
        """
        """
        logger.setLevel(self.level)
        if self.workflow:
            logger.addHandler(self.file_handler)
        if self.streem_on:
            logger.addHandler(self.stream_handler)
