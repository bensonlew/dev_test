# -*- coding: utf-8 -*-
# __author__ = 'guoquan'
import web
import mainapp.core.auto_load as autoload
from biocluster.core.function import hostname
from mainapp.libs.signature import check_sig
from mainapp.controllers.pipeline import Pipeline, PipelineState, PipelineLog, PipelineStop, PipelineQueue, PipelineStopPause, PipelinePause
from mainapp.controllers.filecheck import FileCheck, MultiFileCheck
from mainapp.controllers.report.download_web_pic import DownloadWebPic
from mainapp.controllers.instant.dataexchange.download_task import DownloadTask
from mainapp.controllers.instant.dataexchange.upload_task import UploadTask
from mainapp.controllers.submit.meta.pipe import Pipe


# Meta instant
from mainapp.controllers.instant.meta.two_group import TwoGroup
from mainapp.controllers.instant.meta.estimators import Estimators
from mainapp.controllers.instant.meta.pan_core import PanCore
from mainapp.controllers.instant.meta.venn import Venn
from mainapp.controllers.instant.meta.beta.hcluster import Hcluster
from mainapp.controllers.instant.meta.otu_subsample import OtuSubsample
from mainapp.controllers.instant.meta.two_sample import TwoSample
from mainapp.controllers.instant.meta.multiple import Multiple
from mainapp.controllers.instant.meta.est_t_test import EstTTest
from mainapp.controllers.instant.meta.beta.multi_analysis import MultiAnalysis
from mainapp.controllers.instant.meta.beta.anosim import Anosim
from mainapp.controllers.instant.meta.plot_tree import PlotTree
from mainapp.controllers.instant.meta.corr_network import CorrNetwork
from mainapp.controllers.instant.meta.pearson_correlation import PearsonCorrelation
from mainapp.controllers.instant.meta.cluster_analysis import ClusterAnalysis
from mainapp.controllers.instant.meta.mantel_test import MantelTest
from mainapp.controllers.instant.meta.hierarchical_clustering_heatmap import HierarchicalClusteringHeatmap
from mainapp.controllers.instant.meta.enterotyping import Enterotyping
from mainapp.controllers.instant.meta.demo_mongodata_copy import DemoMongodataCopy
from mainapp.controllers.instant.meta.convert_level import ConvertLevel

# Meta submit
from mainapp.controllers.submit.meta.lefse import Lefse
from mainapp.controllers.submit.meta.rarefaction import Rarefaction
from mainapp.controllers.submit.meta.randomforest import Randomforest
from mainapp.controllers.submit.meta.roc import Roc
from mainapp.controllers.submit.meta.metagenomeseq import Metagenomeseq
from mainapp.controllers.submit.meta.otunetwork import Otunetwork
from mainapp.controllers.submit.meta.n_pca import NPca
from mainapp.controllers.submit.meta.environmental_regression import EnvironmentalRegression
from mainapp.controllers.submit.meta.function_predict import FunctionPredict
from mainapp.controllers.submit.meta.meta_sourcetracker import MetaSourcetracker

# Denovo_rna submit
from mainapp.controllers.submit.denovo_rna.diff_express import DiffExpress
from mainapp.controllers.submit.denovo_rna.cluster import Cluster
from mainapp.controllers.submit.denovo_rna.go_enrich_regulate import GoEnrichRegulate
from mainapp.controllers.submit.denovo_rna.network import Network

# Denovo_rna instant
from mainapp.controllers.instant.denovo_rna.denovo_venn import DenovoVenn


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
    "/download/report/pdf", "DownloadWebPic",
    "/download/report/png", "DownloadWebPic",
    "/dataexchange/download_task", "DownloadTask",
    "/app/dataexchange/download_task", "DownloadTask",
    "/dataexchange/upload_task", "UploadTask",
    "/app/dataexchange/upload_task", "UploadTask",
    "/meta/pipe", "Pipe",

    # Meta
    "/meta/demo_mongodata_copy", "DemoMongodataCopy",
    "/meta/convert_level", "ConvertLevel",
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
    "/meta/beta/anosim", "Anosim",
    "/meta/plot_tree", "PlotTree",
    "/meta/randomforest", "Randomforest",
    "/meta/roc", "Roc",
    "/meta/metagenomeseq", "Metagenomeseq",
    "/meta/corr_network", "CorrNetwork",
    "/meta/otu_network", "Otunetwork",
    "/meta/n_pca", "NPca",
    "/meta/pearson_correlation", "PearsonCorrelation",
    "/meta/cluster_analysis", "ClusterAnalysis",
    "/meta/environmental_regression", "EnvironmentalRegression",
    "/meta/mantel_test", "MantelTest",
    "/meta/hierarchical_clustering_heatmap", "HierarchicalClusteringHeatmap",
    "/meta/enterotyping", "Enterotyping",
    "/meta/function_predict", "FunctionPredict",
    "/meta/meta_sourcetracker", "MetaSourcetracker",

    # denovo_rna
    "/denovo_rna/network", "Network",
    "/denovo_rna/cluster", "Cluster",
    "/denovo_rna/diff_express", "DiffExpress",
    "/denovo_rna/denovo_venn", "DenovoVenn",
    "/denovo_rna/go_enrich_regulate", "GoEnrichRegulate"
)


class hello(object):
    # @check_sig
    def GET(self):
        return "%s %s" % (hostname, web.ctx.homedomain)

app = web.application(urls, globals())
autoload.register(app)
application = app.wsgifunc()
