#!/mnt/ilustre/users/sanger/app/Python/bin/python
from mbio.workflows.single import SingleWorkflow
from biocluster.wsheet import Sheet

wsheet = Sheet("single_blast.json")
wf = SingleWorkflow(wsheet)
wf.run()