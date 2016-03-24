# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import web
from mainapp.libs.signature import check_sig
from mainapp.controllers.pipeline import Pipeline, PipelineState,PipelineLog,PipelineStop,PipelineQueue,PipelineStopPause,PipelinePause
from mainapp.controllers.filecheck import FileCheck, MultiFileCheck
from mainapp.controllers.meta.two_group import TwoGroup
from mainapp.controllers.meta.two_sample import TwoSample
from mainapp.controllers.meta.multiple import Multiple
from mainapp.controllers.filecheck import FileCheck,MultiFileCheck
from mainapp.controllers.meta.estimators import Estimators
from mainapp.controllers.meta.pan_core import PanCore
from mainapp.controllers.meta.venn import Venn
from mainapp.controllers.meta.heat_cluster import HeatCluster
from mainapp.controllers.meta.beta.distance_calc import DistanceCalc
from mainapp.controllers.meta.beta.hcluster import Hcluster
from mainapp.controllers.meta.otu_subsample import Subsample
from mainapp.controllers.meta.est_t_test import EstTTest
from mainapp.controllers.meta.rarefaction import Rarefaction


# web.config.debug = False
urls = (
    "/hello", "hello",
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
    "/meta/estimators", "Estimators",
    "/pipeline/stop_pause", "PipelineStopPause",
    "/meta/pan_core", "PanCore",
    "/meta/venn", "Venn",
    "/meta/heat_cluster", "HeatCluster",
    "/meta/beta/distance_calc", "DistanceCalc",
    "/meta/beta/hcluster", "Hcluster",
    "/meta/otu_subsample", "Subsample",
    "/meta/two_group", "TwoGroup",
    "/meta/two_sample", "TwoSample",
    "/meta/multiple", "Multiple",
    "/meta/rarefaction", "Rarefaction",
    "/meta/est_t_test", "EstTTest"
)


class hello(object):
    @check_sig
    def GET(self):
        return "zzz"


application = web.application(urls, globals()).wsgifunc()
