import json


class Speed(object):
    def __init__(self, channel: int = 1):
        self.channel = channel
        self.byte = -1
        self.record = -1

    def to_dict(self) -> dict:
        return {'channel': self.channel, 'byte': self.byte, 'record': self.record}


class ErrorLimit(object):
    def __init__(self):
        self.record = 0
        self.percentage = 0.02

    def to_dict(self) -> dict:
        return {'record': self.record, 'percentage': self.percentage}


class Setting(object):
    def __init__(self, speed: Speed, error_limit: ErrorLimit):
        self.speed = speed
        self.error_limit = error_limit

    def to_dict(self) -> dict:
        return {'speed': self.speed.to_dict(), 'errorLimit': self.error_limit.to_dict()}


class Content(object):
    def __init__(self, reader: dict, writer: dict):
        self.reader = reader
        self.writer = writer

    def to_dict(self) -> dict:
        return {'reader': self.reader, 'writer': self.writer}


class Job(object):
    def __init__(self, setting: Setting, content: Content):
        self.setting = setting
        self.content = content

    def to_dict(self) -> dict:
        return {'setting': self.setting.to_dict(), 'content': [self.content.to_dict()]}


class DataxJson(object):
    def __init__(self, job: Job):
        self.job = job

    def to_dict(self) -> dict:
        return {'job': self.job.to_dict()}


class BuildJobJson(object):
    def __init__(self, source_name: str, db_type: str, parallel: int, datax_json: DataxJson):
        self.source_name = source_name
        self.db_type = db_type
        self.parallel = parallel
        self.datax_json = datax_json

    def to_dict(self):
        return {'source_name': self.source_name, 'db_type': self.db_type, 'parallel': self.parallel,
                'datax_json': self.datax_json.to_dict()}

    def __str__(self) -> str:
        return json.dumps(self.to_dict(), indent=4, separators=(',', ':'))