class Message(object):
    def __init__(self, job_id, content):
        self._job_id = job_id
        self._content = content

    @property
    def job_id(self):
        return self._job_id

    @property
    def content(self):
        return self._content


