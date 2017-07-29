from multiprocessing.managers import Server, BaseManager, State
from multiprocessing import current_process
import threading


class NewServer(Server):
    def __init__(self, registry, address, authkey, serializer):
        super(NewServer, self).__init__(registry, address, authkey, serializer)
        self.to_stop = False

    def server_until_stop(self):
        current_process()._manager_server = self
        try:
            try:
                while self.to_stop is False:
                    try:
                        c = self.listener.accept()
                    except (OSError, IOError):
                        continue
                    t = threading.Thread(target=self.handle_request, args=(c,))
                    t.daemon = True
                    t.start()
            except (KeyboardInterrupt, SystemExit):
                pass
            except Exception:
                pass
        finally:
            self.stop = 999
            self.listener.close()


class NewBaseManager(BaseManager):

    def get_server(self):
        assert self._state.value == State.INITIAL
        return NewServer(self._registry, self._address, self._authkey, self._serializer)
