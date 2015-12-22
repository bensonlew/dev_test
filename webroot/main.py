# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import web
from mainapp.libs.signature import check_sig
from mainapp.controllers.pipeline import Pipeline, PipelineState,PipelineLog,PipelineStop,PipelineQueue,PipelineStopPause,PipelinePause
from mainapp.controllers.filecheck import FileCheck

# web.config.debug = False
urls = (
    "/hello", "hello",
    "/filecheck", "FileCheck",
    "/pipeline", "Pipeline",
    "/pipeline/state", "PipelineState",
    "/pipeline/log", "PipelineLog",
    "/pipeline/stop", "PipelineStop",
    "/pipeline/running", "PipelineRunning",
    "/pipeline/queue", "PipelineQueue"
    "/pipeline/pause", "PipelinePause"
    "/pipeline/stop_pause", "PipelineStopPause"
    )


class hello(object):
    @check_sig
    def GET(self):
        return "xxxx"

if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()