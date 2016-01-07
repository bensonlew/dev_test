# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
from pymongo import MongoClient


class Base(object):
    def __init__(self):
        self._bind_object = None
        self._client = None
        self._db = None

    def __get__(self, instance, owner):
        self._bind_object = instance
        return self

    @property
    def bind_object(self):
        return self._bind_object
