# -*- coding: utf-8 -*-
# __author__ = 'guhaidong'
# last_modify:20170921
from biocluster.api.database.base import Base, report_check
import os
import datetime
import types
from biocluster.config import Config
from bson.son import SON
from bson.objectid import ObjectId


class Assemble(Base):
    def __init__(self, bind_object):
        super(Assemble, self).__init__(bind_object)
        self.__db__name = Config().MONGODB + '_metagenomic'

    @report_check
    def add_assemble_stat(self):


    @report_check
    def add_assemble_stat_detail(self):

    @report_check
    def add_assem_bar_detail(self):