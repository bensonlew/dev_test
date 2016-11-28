# -*- coding: utf-8 -*-
# __author__ = 'guoquan'



# class Listener(Process):
#     def __init__(self, **kwargs):
#         super(Listener, self).__init__(**kwargs)
#         self.manager = TaskManager()
#
#     def run(self):
#         super(Listener, self).run()
#         ListenManager.register("Task", TaskManager)
#         ListenManager.register("get_event", get_event)
#         m = ListenManager(address=('127.0.0.1', 6789), authkey='abracadabra')
#         s = m.get_server()
#         s.serve_forever()
