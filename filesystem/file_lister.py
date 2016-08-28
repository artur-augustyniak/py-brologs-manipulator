# -*- coding: utf-8 -*-
import os
import re
from datetime import timedelta

FROM_HOUR_DEF = '00'
TO_HOUR_DEF = '23'
DATE_FORMAT = "%Y-%m-%d"
BRO_LOG_NAME_PATTERN = "http.*.log.gz"
BRO_LOGS_PATH = "/share/http_log"


class FileLister(object):
    def __init__(
            self,
            start_date,
            end_date,
            path=None,
            name_wildcard=None,
            from_hour=FROM_HOUR_DEF,
            to_hour=TO_HOUR_DEF
    ):
        self.start = start_date
        self.end = end_date
        self.path = path

        if not os.path.isdir(self.path):
            raise IOError(self.path + " is not directory")

        self.wildcard = name_wildcard if name_wildcard is not None else ".*"
        self.from_hour = from_hour
        self.to_hour = to_hour

    @property
    def files(self):

        for root, dirs, files in os.walk(self.path, topdown=False):
            for name in files:
                if re.match(self.wildcard, name) is not None:
                    path = os.path.join(root, name)
                    if any(x in path for x in
                           self.date_range(self.start, self.end)) \
                            and \
                            any(x in path for x in
                                self.time_range(self.from_hour, self.to_hour)):
                        yield path

    @staticmethod
    def date_range(start_date, end_date):
        for n in xrange(int((end_date - start_date).days + 1)):
            yield str(start_date + timedelta(n))

    @staticmethod
    def time_range(from_hour, to_hour):
        for n in xrange(int(from_hour), int(to_hour) + 1):
            yield str(n).zfill(2) + '-00-00_'
