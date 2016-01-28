# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import web
from mainapp.libs.signature import check_sig
from mainapp.controllers.pipeline import Pipeline, PipelineState,PipelineLog,PipelineStop,PipelineQueue,PipelineStopPause,PipelinePause
from mainapp.controllers.filecheck import FileCheck,MultiFileCheck
from mainapp.controllers.meta.beta.distance_calc import DistanceCalc
from mainapp.controllers.meta.beta.hcluster import Hcluster

# web.config.debug = False
urls = (
    "/hello", "hello",
    "/meta/beta/distance_calc", "DistanceCalc",
    "/filecheck", "FileCheck",
    "/filecheck/multi", "MultiFileCheck",
    "/pipeline", "Pipeline",
    "/pipeline/state", "PipelineState",
    "/pipeline/log", "PipelineLog",
    "/pipeline/stop", "PipelineStop",
    "/pipeline/running", "PipelineRunning",
    "/pipeline/queue", "PipelineQueue",
    "/pipeline/pause", "PipelinePause",
    "/pipeline/stop_pause", "PipelineStopPause",
    "/meta/beta/hcluster", "Hcluster"
)


class hello(object):
    @check_sig
    def GET(self):
        return "zzz"


application = web.application(urls, globals()).wsgifunc()
