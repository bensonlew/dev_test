# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import web
from mainapp.libs.signature import check_sig
from mainapp.controllers.pipeline import Pipeline, PipelineState,PipelineLog,PipelineStop,PipelineQueue,PipelineStopPause,PipelinePause
from mainapp.controllers.filecheck import FileCheck, MultiFileCheck
from mainapp.controllers.instant.meta.two_group import TwoGroup
from mainapp.controllers.instant.meta.estimators import Estimators
from mainapp.controllers.instant.meta.pan_core import PanCore
from mainapp.controllers.instant.meta.venn import Venn
from mainapp.controllers.instant.meta.heat_cluster import HeatCluster
from mainapp.controllers.instant.meta.beta.distance_calc import DistanceCalc
from mainapp.controllers.instant.meta.beta.hcluster import Hcluster
from mainapp.controllers.instant.meta.otu_subsample import OtuSubsample
from mainapp.controllers.instant.meta.two_sample import TwoSample
from mainapp.controllers.instant.meta.multiple import Multiple
from mainapp.controllers.submit.meta.lefse import Lefse
from mainapp.controllers.instant.meta.est_t_test import EstTTest
from mainapp.controllers.submit.meta.rarefaction import Rarefaction
from mainapp.controllers.instant.meta.beta.multi_analysis import MultiAnalysis
from mainapp.controllers.instant.meta.beta.anosim import Anosim

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
    "/meta/otu_subsample", "OtuSubsample",
    "/meta/two_group", "TwoGroup",
    "/meta/two_sample", "TwoSample",
    "/meta/multiple", "Multiple",
    "/meta/lefse", "Lefse",
    "/meta/est_t_test", "EstTTest",
    "/meta/rarefaction", "Rarefaction",
    "/meta/beta/multi_analysis", "MultiAnalysis",
    "/meta/beta/anosim", "Anosim"
)


class hello(object):
    @check_sig
    def GET(self):
        return "xxxx"

if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()
