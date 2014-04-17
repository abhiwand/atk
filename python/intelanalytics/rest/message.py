class Message(object):
    def __init__(self, job_id, status_code, message):
        self._job_id = job_id
        self._status_code = status_code
        self._message = message

    @property
    def job_id(self):
        return self._job_id

    @property
    def status_code(self):
        return self._status_code

    @property
    def message(self):
        return self._message


