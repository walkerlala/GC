class PsuQueue:
    """class to support url queue
    """
    def __init__(self, file_name):
        self._queue = []
        self._file = open(file_name, 'r')
        for line in self._file:
            self._queue.append(line)

    def pop_url(self):
        return self._queue.pop(0)

