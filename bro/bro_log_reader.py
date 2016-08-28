# -*- coding: utf-8 -*-
import gzip

BRO_DELIMITER = '\t'
BRO_COMMENT = '#'
BRO_LAST_HEADER_LINE = '#fields'


class BroLogReader:
    def __init__(
            self,
            delimiter=BRO_DELIMITER,
            comment_ind=BRO_COMMENT,
            header_mark=BRO_LAST_HEADER_LINE
    ):
        self.delimiter = delimiter
        self.comment_ind = comment_ind
        self.header_mark = header_mark

    def lines(self, logfile):
        bro_fptr = self._omit_bro_header(logfile)
        for line in bro_fptr:
            if not line.startswith(self.comment_ind):
                yield line.strip().split(self.delimiter)

    def _omit_bro_header(self, logfile):
        f = gzip.open(logfile, 'rb')
        c = next(f)
        while not c.startswith(self.header_mark):
            c = next(f)
        next(f)
        return f
