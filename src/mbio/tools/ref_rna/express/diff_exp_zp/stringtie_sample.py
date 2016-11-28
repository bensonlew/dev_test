#!/mnt/ilustre/users/sanger/app/Python/bin/python
from mbio.workflows.single import SingleWorkflow
from biocluster.wsheet import Sheet


wsheet = Sheet("sample_zhangpeng.json")
wf = SingleWorkflow(wsheet)
wf.run()