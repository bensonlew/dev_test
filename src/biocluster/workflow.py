# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
"""workflow工作流类模块"""

from .core.function import load_class_by_path, get_action_queue, add_run_queue, add_log_queue
from .basic import Basic
from .config import Config
import os
import sys
from .rpc import RPC, LocalServer
from .logger import Wlog
from .agent import Agent
from .module import Module
import datetime
import gevent
import time
from biocluster.api.file.remote import RemoteFileManager
from biocluster.api.database.base import ApiManager
import re
import importlib
import types
import traceback
from .core.watcher import Watcher
from .scheduling.job import JobManager
# # from gevent.lock import BoundedSemaphore
# # import traceback
# # from .wpm.client import worker_client, log_client
# import json
from .wpm.db import ReportModel
from .core.exceptions import MaxLengthError


class Workflow(Basic):
    """
    工作流程基类
    """

    def __init__(self, wsheet, **kwargs):
        if "debug" in kwargs.keys():
            self.debug = kwargs["debug"]
        else:
            self.debug = False

        super(Workflow, self).__init__(**kwargs)
        if wsheet.WPM:
            self.action_queue = wsheet.action_queue
            self.run_info_queue = wsheet.run_info_queue
            self.log_info_queue = wsheet.log_info_queue
            self.rpc_message_queue = wsheet.rpc_message_queue
            self.rpc_callback_queue = wsheet.rpc_callback_queue
            self.rpc_restart_signal = wsheet.rpc_restart_signal
        self.sheet = wsheet
        # self._return_mongo_ids = []  # 在即时计算情况下，需要返回写入mongo库的主表ids，用于更新sg_status表，
        # 值为三个元素的字典{'collection_name': '', 'id': ObjectId(''), 'desc': ''}组成的列表

        self._return_msg = []  # 需要返回给任务调用进程的值,支持常用数据类型
        self.last_update = datetime.datetime.now()
        if "parent" in kwargs.keys():
            self._parent = kwargs["parent"]
        else:
            self._parent = None

        self.pause = False
        self._pause_time = None
        # self.USE_DB = False
        self.__json_config()

        self._id = wsheet.id
        self.config = Config()
        # self.db = self.config.get_db()
        # self.db_sem = BoundedSemaphore(1)
        self._work_dir = self.__work_dir()
        self._output_path = self._work_dir + "/output"
        self._tools_report_data = []
        self.add_event('rpctimeout')  # RPC接收信息超时
        self.on("rpctimeout", self._event_rpctimeout)
        self.last_get_message = None
        if not self.debug:
            if not os.path.exists(self._work_dir):
                os.makedirs(self._work_dir)
            if not os.path.exists(self._output_path):
                os.makedirs(self._output_path)
            self.__check_to_file_option()
            self.step_start()
        self._logger = Wlog(self).get_logger("")
        if self.sheet.instant is True:
            # self.USE_DB = False
            self.rpc_server = LocalServer(self)
        else:
            self.rpc_server = RPC(self)

    def __json_config(self):
        # if self.sheet.USE_DB is True:
        #     self.USE_DB = True
        if self.sheet.UPDATE_STATUS_API is not None:
            self.UPDATE_STATUS_API = self.sheet.UPDATE_STATUS_API
        if self.sheet.IMPORT_REPORT_DATA is True:
            self.IMPORT_REPORT_DATA = True
        if self.sheet.IMPORT_REPORT_AFTER_END is True:
            self.IMPORT_REPORT_AFTER_END = True

    def _event_rpctimeout(self):
        """
        RPC信息接收时间超时时触发

        :return:
        """
        if self.last_get_message:
            time_last = (datetime.datetime.now() - self.last_get_message).seconds
            if time_last < 30:
                self.logger.debug("RPC服务%s秒钟以前接收到信息，应该还在正常工作，忽略重启!" % time_last)
                return
        self.rpc_server.restart()

    def add_tool_report(self, data):
        """
        添加Tool运行报告
        :return:
        """
        self._tools_report_data.append(data)

    def step_start(self):
        """
        流程开始api更新
        :return:
        """
        self.step.start()
        self.step.update()

    def __work_dir(self):
        """
        获取并创建工作目录
        """
        work_dir = self.config.WORK_DIR
        timestr = str(time.strftime('%Y%m%d', time.localtime(time.time())))
        work_dir = work_dir + "/" + timestr + "/" + self.name + "_" + self._id
        return work_dir

    def __check_to_file_option(self):
        """
        转换内置的参数为文件参数

        :return:
        """
        if "to_file" in self._sheet.data.keys():
            data = self._sheet.data["to_file"]
            if isinstance(data, types.StringTypes):
                to_files = [data]
            else:
                to_files = data
            for opt in to_files:
                m = re.match(r"([_\w\.]+)\((.*)\)", opt)
                if m:
                    func_path = m.group(1)
                    f_list = func_path.split(".")
                    func_name = f_list.pop()
                    lib_path = ".".join(f_list)
                    options = m.group(2)
                    opt_list = re.split(r"\s*,\s*", options)
                    for optl in opt_list:
                        if re.match(r"^biocluster\.api\.to_file", lib_path):
                            imp = importlib.import_module("%s" % lib_path)
                        else:
                            imp = importlib.import_module("mbio.api.to_file.%s" % lib_path)
                        func = getattr(imp, func_name)
                        self._sheet.set_option(optl, func(self._sheet.option(optl), optl, self.work_dir, self))
                else:
                    imp = importlib.import_module("biocluster.api.to_file.parameter")
                    func = getattr(imp, "json_to_file")
                    self._sheet.set_option(opt, func(self._sheet.option(opt), opt, self.work_dir, self))

                # json_data = self._sheet.option(opt)
                # file_path = os.path.join(self.work_dir, "%s_input.json" % opt)
                # with open(file_path, "w") as f:
                #     json.dump(json_data, f, indent=4)

    def add_module(self, path):
        """
        添加下属 :py:class:`biocluster.module.Module`

        :param path:  :py:class:`biocluster.module.Module` 对应的自动加载path路径，请参考教程中对应的说明

        """
        module = load_class_by_path(path, tp="Module")(self)
        self.add_child(module)
        return module

    def add_tool(self, path):
        """
        直接添加下属 :py:class:`biocluster.agent.Agent`

        :param path: String   :py:class:`biocluster.agent.Agent` 动态加载path路径
        :return:  Agent 返回Tool对应的 :py:class:`biocluster.agent.Agent` 对象
        """
        tool = load_class_by_path(path, tp="Agent")(self)
        self.add_child(tool)
        return tool

    def find_tool_by_id(self, toolid):
        """
        通过id搜索所属Tool

        :param toolid:  :py:class:`biocluster.tool.Tool` 对象的ID
        """
        # ids = toolid.split(".")
        # length = len(ids)
        # if length < 2 or length > 3:
        #     return False
        # if ids[0] != self.id:
        #     return False
        modules = self.children
        # if length == 2:
        for md in modules:
            if md.id == toolid and isinstance(md, Agent):
                return md
            elif isinstance(md, Module):
                tool = md.find_tool_by_id(toolid)
                if tool:
                    return tool
        return False

    def run(self):
        """
        开始运行

        :return:
        """
        super(Workflow, self).run()
        if self.sheet.instant is not True and self.sheet.WPM:
            watcher = Watcher()
            watcher.add(self.__check, 2)
        self.rpc_server.run()

    def end(self):
        """
        停止workflow运行

        :return:
        """
        super(Workflow, self).end()
        self.end_unfinish_job()
        self._upload_result()
        self._import_report_data()
        self._update("set_end")
        self.step.finish()
        self.step.update()
        self._save_report_data()
        self.rpc_server.close()
        self.logger.info("workflow运行结束!")

    def set_return_msg(self, msg):
        """
        设置返回信息给WPM，使用WorkflowManager.get_msg方法可在运行完成后获取此信息，只能获取一次


        :param msg: 需要传递的信息，支持常用数据类型
        :return:
        """
        self._return_msg = msg

    def add_return_mongo_id(self, collection_name, table_id, desc='', add_in_sg_status=True):
        """
        为了兼容旧版本,新版本请使用set_return_msg

        :param collection_name:
        :param table_id:
        :param desc:
        :param add_in_sg_status:
        :return:
        """
        return_dict = dict()
        return_dict['id'] = table_id
        return_dict['collection_name'] = collection_name
        return_dict['desc'] = desc
        return_dict['add_in_sg_status'] = add_in_sg_status
        self._return_msg.append(return_dict)

    def _upload_result(self):
        """
        上传结果文件到远程路径

        :return:
        """
        if self._sheet.output:
            for up in self.upload_dir:
                # target_dir = os.path.join(self.sheet.output, os.path.dirname(up.upload_path))
                target_dir = self.sheet.output
                # 去掉target目录加上了上传相对路径的路径差 即 上传目录 与工作目录的路径差
                remote_file = RemoteFileManager(target_dir)
                self.logger.info("开始上传%s到%s" % (up.path, target_dir))
                if remote_file.type != "local":
                    remote_file.upload(up.path)
                self.logger.info("上传%s到%s完成" % (up.path, target_dir))

    def _import_report_data(self):
        if self.IMPORT_REPORT_DATA is True and self.IMPORT_REPORT_AFTER_END is True:
            api_call_list = self.api.get_call_records_list()
            if len(api_call_list) > 0:
                self.logger.info("开始导入Report数据...")
                api_manager = ApiManager(self, play_mod=True, debug=self.debug)
                api_manager.load_call_records_list(api_call_list)
                api_manager.play()
                self.logger.info("导入Report数据结束！")

    def exit(self, exitcode=1, data="", terminated=False):
        """
        立即退出当前流程

        :param exitcode:
        :param data:
        :param terminated:
        :return:
        """
        if terminated:
            self.step.terminated(data)
        else:
            self.step.failed(data)
        self.end_unfinish_job()
        self.step.update()
        self._update("set_error", "程序主动退出:%s" % data)
        self._save_report_data()
        self.rpc_server.close()
        self.logger.info("程序退出: %s " % data)
        sys.exit(exitcode)

    # def __update_service(self):
    #     """
    #     每隔30s定时更新数据库last_update时间
    #
    #     :return:
    #     """
    #     # while self.is_end is False:
    #     #     gevent.sleep(60)
    #     if self.is_end is True:
    #         return "exit"
    #     self._update("online")
    #     # with self.db_sem:
    #     # try:
    #     #     with self.db.transaction():
    #     #         self.db.query("UPDATE workflow SET last_update=CURRENT_TIMESTAMP where workflow_id=$id",
    #     #                       vars={'id': self._id})
    #     # except Exception, e:
    #     #     exstr = traceback.format_exc()
    #     #     print exstr
    #     #     self.logger.debug("数据库更新异常: %s" % e)
    #     # finally:
    #     #     self.close_db_cursor()

    def _update(self, action, msg=None):
        """
        插入数据库，更新流程运行状态,只在后台服务调用时生效

        :param action: 更新信息类型
        :return:
        """
        # if self.USE_DB:
        #     try:
        #         myvar = dict(id=self._id)
        #         self.db.update("workflow", vars=myvar, where="workflow_id = $id", **data)
        #     except Exception, e:
        #         exstr = traceback.format_exc()
        #         print exstr
        #         self.logger.debug("数据库更新异常: %s" % e)
        #     finally:
        #         self.close_db_cursor()

        if self.sheet.WPM:
            # try:
            #     worker = worker_client()
            #     if type == "end":
            #         self.run_info_queue.put((type, (self.sheet.id, self._return_msg)))
            #     elif type == "keepalive":
            #         # worker.keep_alive(self.sheet.id)
            #         self.run_info_queue.put((type, (self.sheet.id, )))
            #     elif type == "error":
            #         # worker.set_error(self.sheet.id, msg)
            #         self.run_info_queue.put((type, (self.sheet.id,)))
            #     elif type == "pause":
            #         worker.set_pause(self.sheet.id)
            #     elif type == "stop":
            #         worker.set_stop(self.sheet.id)
            #     elif type == "pause_exit":
            #         worker.set_pause_exit(self.sheet.id)
            #     elif type == "pause_timeout":
            #         worker.pause_timeout(self.sheet.id)
            #     elif type == "report":
            #         worker.report(self.sheet.id, msg)
            # except Exception, e:
            #     exstr = traceback.format_exc()
            #     print exstr
            #     sys.stdout.flush()
            #     self.logger.error("连接WPM服务异常: %s," % e)
            # else:
            #     del worker
            if action == "set_end":
                add_run_queue(self.run_info_queue, "set_end", self.sheet.id, self._return_msg)
            elif action == "set_error":
                add_run_queue(self.run_info_queue, action, self.sheet.id, msg)
            else:
                add_run_queue(self.run_info_queue, action, self.sheet.id)

    # def __add_run_queue(self, action, data, msg=None):
    #     # length = len(self.run_info_queue)
    #     is_full = True
    #     with self.run_info_queue.get_lock():
    #         # if self.run_info_queue[length - 1].action:
    #         #     time.sleep(0.3)
    #         #     self.__add_run_queue(action, data, msg)
    #         #     return
    #         for i, v in enumerate(self.run_info_queue):
    #             if self.run_info_queue[i].action is None:
    #                 is_full = False
    #                 if msg is not None:
    #                     msg = json.dumps(msg, cls=CJsonEncoder)
    #                 self.run_info_queue[i] = (action, data, msg)
    #                 break
    #     if is_full:
    #         time.sleep(0.3)
    #         self.__add_run_queue(action, data, msg)
    #         return

    # def _add_log_queue(self, action, data, msg=None):
    #     # length1 = len(self.log_info_queue)
    #     is_full = True
    #     with self.log_info_queue.get_lock():
    #         # if self.log_info_queue[length1 - 1].action:
    #         #     time.sleep(0.3)
    #         #     self._add_log_queue(action, data, msg)
    #         #     return
    #         for i, v in enumerate(self.log_info_queue):
    #             if self.log_info_queue[i].action is None:
    #                 is_full = False
    #                 if msg is not None:
    #                     msg = json.dumps(msg, cls=CJsonEncoder)
    #                 self.log_info_queue[i] = (action, data, msg)
    #                 break
    #     if is_full:
    #         time.sleep(0.3)
    #         self._add_log_queue(action, data, msg)
    #         return

    def send_log(self, data):
        """
        发送API LOG信息到WPM API LOG管理器

        ;:param data: 需要发送的数据
        :return:
        """
        if self.sheet.WPM:
            #
            # try:
            #     log = log_client()
            #     log.add_log(data)
            # except Exception, e:
            #     exstr = traceback.format_exc()
            #     print exstr
            #     sys.stdout.flush()
            #     self.logger.error("连接WPM log服务异常: %s" % e)
            # else:
            #     del log
            # self.log_info_queue.put(("add_log", (data, )))
            # json_str = json.dumps(data, cls=CJsonEncoder)
            add_log_queue(self.log_info_queue, "add_log", data)

    def __check(self):
        if self.is_end is True:
            return "exit"

        now = datetime.datetime.now()
        if self.pause:
            if (now - self._pause_time).seconds > self.config.MAX_PAUSE_TIME:
                self._update("pause_timeout")
                self.exit(data="暂停超过规定的时间%ss,自动退出运行!" %
                               self.config.MAX_PAUSE_TIME, terminated=True)
        if (now - self.last_update).seconds > (self.config.MAX_WAIT_TIME * 3 + 100):
            self.exit(data="超过 %s s没有任何运行更新，退出运行！" % (self.config.MAX_WAIT_TIME * 3 + 100))
        action = get_action_queue(self.action_queue, self.sheet.id)
        if action == "tostop":
            self._stop()
        elif action == "pause":
            self._pause()
        elif action == "exit_pause":
            self._exit_pause()
        if action is not None:
            self.logger.debug("接收到指令:%s" % action)

    def _stop(self):
        self.logger.info("接收到终止运行指令。")
        self._update("set_stop")
        self.exit(data="接收到终止运行指令，退出运行!", terminated=True)

    def _pause(self):
        self.pause = True
        self._pause_time = datetime.datetime.now()
        self.step.pause()
        self.step.update()
        self._update("set_pause")
        self.logger.info("检测到暂停指令，暂停所有新模块运行")

    def _exit_pause(self):
        self.pause = False
        self._pause_time = None
        self.step.start()
        self.step.update()
        self._update("set_pause_exit")
        self.logger.info("检测到恢复运行指令，恢复所有模块运行!")

    def _save_report_data(self):
        if self.sheet.WPM:
            self.logger.debug("开始发送运行数据!")
            data = dict()
            data["modules"] = []
            for child in self.children:
                if isinstance(child, Module):
                    data["modules"].append(child.get_report_data())
            data["tools"] = self._tools_report_data
            data["id"] = self.sheet.id
            try:
                add_log_queue(self.log_info_queue, "report", data)
                self.logger.debug("运行数据发送完成!")
            except MaxLengthError, e:
                self.logger.debug("保存到Log共享内存出错，直接保存到Mysql: %s" % e)
                self.__report_to_mysql(data)
            except Exception, e:
                exstr = traceback.format_exc()
                print exstr
                sys.stdout.flush()
                self.logger.error("保存运行报告异常: %s" % e)

    def __report_to_mysql(self, data):
        """
        保存运行数据
        :return:
        """
        wid = data["id"]
        self.logger.info("开始保存%s报告..." % wid)
        model = ReportModel(wid)
        try:
            if len(data["modules"]) > 0:
                model.save_modules(data["modules"])
            if len(data["tools"]) > 0:
                model.save_tools(wid, data["tools"], commit=True, under_workflow=1)
            self.logger.info("Workflow %s 保存运行报告完成" % wid)
        except Exception, e:
            exstr = traceback.format_exc()
            print exstr
            sys.stdout.flush()
            self.logger.error("保存运行报告异常: %s" % e)
        finally:
            model.close()

    # def __check_tostop(self):
    #     """
    #     检查数据库的停止指令，如果收到则退出流程
    #
    #     :return:
    #     """
    #     # while self.is_end is False:
    #     # if self.is_end is True:
    #     #     return "exit"
    #     # if (datetime.datetime.now() - self.last_update).seconds > self.config.MAX_WAIT_TIME:
    #     #     self.exit(data="超过 %s s没有任何运行更新，退出运行！" % self.config.MAX_WAIT_TIME)
    #     # gevent.sleep(10)
    #     # myvar = dict(id=self._id)
    #     # try:
    #     #     results = self.db.query("SELECT * FROM tostop "
    #     #                             "WHERE workflow_id=$id and done  = 0", vars={'id': self._id})
    #     #     if isinstance(results, long) or isinstance(results, int):
    #     #         self.close_db_cursor()
    #     #         gevent.sleep(10)
    #     #         return
    #     #     if len(results) > 0:
    #     #         data = results[0]
    #     #         update_data = {
    #     #             "stoptime": datetime.datetime.now(),
    #     #             "done": 1
    #     #         }
    #     #         self.db.update("tostop", vars=myvar, where="workflow_id = $id", **update_data)
    #     #         self.exit(data="接收到终止运行指令,%s" % data.reson, terminated=True)
    #     #
    #     # except Exception, e:
    #     #     exstr = traceback.format_exc()
    #     #     print exstr
    #     #     self.logger.info("查询数据库异常: %s" % e)
    #     # finally:
    #     #     self.close_db_cursor()
    #
    # def __check_pause(self):
    #     """
    #     检查暂停指令或终止暂停指令
    #
    #     :return:
    #     """
    #     if self.is_end is True:
    #         return "exit"
    #     myvar = dict(id=self._id)
    #     try:
    #         results = self.db.query("SELECT * FROM pause WHERE workflow_id=$id and "
    #                                 "has_continue  = 0 and timeout = 0", vars={'id': self._id})
    #         if isinstance(results, long) or isinstance(results, int):
    #             self.close_db_cursor()
    #             gevent.sleep(10)
    #             return
    #         if len(results) > 0:
    #             data = results[0]
    #             if data.has_pause == 0:
    #                 self.pause = True
    #                 self._pause_time = datetime.datetime.now()
    #                 update_data = {
    #                     "pause_time": datetime.datetime.now(),
    #                     "has_pause": 1
    #                 }
    #                 self.db.update("pause", vars=myvar, where="workflow_id = $id", **update_data)
    #                 self.db.query("UPDATE workflow SET paused = 1 where workflow_id=$id", vars={'id': self._id})
    #                 self.step.pause()
    #                 self.step.update()
    #                 self.logger.info("检测到暂停指令，暂停所有新模块运行: %s" % data.reason)
    #             else:
    #                 if data.exit_pause == 0:
    #                     now = datetime.datetime.now()
    #                     if self.pause:
    #                         if (now - self._pause_time).seconds > self.config.MAX_PAUSE_TIME:
    #                             update_data = {
    #                                 "timeout_time": datetime.datetime.now(),
    #                                 "timeout": 1
    #                             }
    #                             self.db.update("pause", vars=myvar, where="workflow_id = $id", **update_data)
    #                             self.db.query("UPDATE workflow SET paused = 0 where workflow_id=$id",
    #                                           vars={'id': self._id})
    #                             self.exit(data="暂停超过规定的时间%ss,自动退出运行!" %
    #                                            self.config.MAX_PAUSE_TIME, terminated=True)
    #                 else:
    #                     if data.has_continue == 0 and data.timeout == 0:
    #                         self.pause = False
    #                         self._pause_time = None
    #                         update_data = {
    #                                 "continue_time": datetime.datetime.now(),
    #                                 "has_continue": 1
    #                         }
    #                         self.db.update("pause", vars=myvar, where="workflow_id = $id", **update_data)
    #                         self.db.query("UPDATE workflow SET paused = 0 where workflow_id=$id",
    #                                       vars={'id': self._id})
    #                         self.step.start()
    #                         self.step.update()
    #                         self.logger.info("检测到恢复运行指令，恢复所有模块运行!")
    #     except Exception, e:
    #         exstr = traceback.format_exc()
    #         print exstr
    #         self.logger.info("查询数据库异常: %s" % e)
    #     finally:
    #         self.close_db_cursor()

    def end_unfinish_job(self):
        """
        结束所有未完成的job任务

        :return:
        """
        manager = JobManager()
        for job in manager.get_unfinish_jobs():
                job.delete()
        if hasattr(self, "process_share_manager"):
            self.process_share_manager.shutdown()

    # def close_db_cursor(self):
    #     cursor = self.db._db_cursor()
    #     cursor.close()
