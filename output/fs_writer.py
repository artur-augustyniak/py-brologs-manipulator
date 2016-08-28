# -*- coding: utf-8 -*-
import os
import sys
import inspect
import csv
import threading

FUCKED_UP_COUNT = 2
PERM_DENIED_ERRNO = 13
FILE_ERRNO = 22


def get_operations():
    classes = {}
    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj) and obj.__doc__ != "abstract":
            classes[obj.__doc__] = obj
    return classes


class Writer(object):
    """abstract"""

    def __init__(self, lister, reader, path):
        self.lister = lister
        self.reader = reader
        self.path = path
        self.push_harder_count = 0

    def proceed(self):
        pass


class SingleFileOutput(Writer):
    """file"""

    def proceed(self):
        # przy pierwszej probie zapisu via
        # mountpoint hdfs nfs gateway dostajemy wyjatek perm denied
        try:
            with open(self.path, 'wb') as f:
                wr = csv.writer(f, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)
                for gzip_file in self.lister.files:
                    print "Processing " + gzip_file
                    for line in self.reader.lines(gzip_file):
                        wr.writerow(line)
        except IOError as e:
            if e.errno == PERM_DENIED_ERRNO and self.push_harder_count < FUCKED_UP_COUNT:
                self.push_harder_count += 1
                self.proceed()
            elif e.errno == FILE_ERRNO:
                raise IOError("dst file already exists")
            else:
                raise e


class MultiFileOutput(Writer):
    """multi-file"""

    lock = threading.Lock()

    def proceed(self):
        for gzip_file in self.lister.files:
            t = threading.Thread(target=self._process_file, args=(gzip_file,))
            t.start()

    def _process_file(self, gzip_path):
        local_push_harder_count = 0
        dest_folder = self.path
        date_folder = self._extract_date(gzip_path)
        hour_part = self._extract_hour(gzip_path)
        full_path = os.path.join(dest_folder, date_folder, hour_part + ".csv")
        # przy pierwszej probie zapisu via
        # mountpoint hdfs nfs gateway dostajemy wyjatek perm denied
        try:
            self._folder_exist(full_path)
            with open(full_path, 'wb') as f:
                wr = csv.writer(f, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)
                print "Processing " + gzip_path
                for line in self.reader.lines(gzip_path):
                    wr.writerow(line)
                print "Finished " + gzip_path
        except EnvironmentError as e:
            if e.errno == PERM_DENIED_ERRNO and local_push_harder_count < FUCKED_UP_COUNT:
                local_push_harder_count += 1
                self._process_file(gzip_path)
            elif e.errno == FILE_ERRNO:
                raise IOError("dst file already exists")
            else:
                print e

    def _folder_exist(self, for_file):
        self.lock.acquire()
        try:
            directory = os.path.dirname(for_file)
            if not os.path.exists(directory):
                os.makedirs(directory)
        finally:
            pass
            self.lock.release()

    @staticmethod
    def _extract_date(gzip_path):
        return os.path.basename(os.path.dirname(gzip_path))

    @staticmethod
    def _extract_hour(gzip_path):
        return os.path.basename(gzip_path)[5:-22]
