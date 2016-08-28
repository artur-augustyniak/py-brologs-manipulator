#!/usr/bin/python
# -*- coding: utf-8 -*-
from optparse import OptionParser
from datetime import datetime

from filesystem import *
from bro import *
from output import *
from console_helper import BCOLORS

OPERATIONS = get_operations()


def parse_dates(options):
    span = int(options.span)
    start_date = datetime.strptime(options.start_date, DATE_FORMAT).date()
    end_date = start_date + timedelta(days=span)
    effective_start_date = min(start_date, end_date)
    effective_end_date = max(start_date, end_date)
    return effective_end_date, effective_start_date


def show_msg(effective_end_date, effective_start_date):
    msg_format = BCOLORS.OKBLUE + "Starting from %s to %s (inclusive)"
    print msg_format % (
        BCOLORS.WARNING + str(effective_start_date) + BCOLORS.ENDC,
        BCOLORS.WARNING + str(effective_end_date) + BCOLORS.ENDC
    )


def prepare_opt_parser():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage)
    parser.add_option("-d", "--date", dest="start_date",
                      help="inclusive start search date - format: " + DATE_FORMAT)
    parser.add_option("-m", "--mode", dest="mode",
                      help=str(OPERATIONS))
    parser.add_option("-o", "--output-path", dest="output_path",
                      help="srararara")
    parser.add_option("-s", "--span", dest="span", default=0,
                      help="extend search to given number days - inclusive")
    parser.add_option("-f", "--from_hour", dest="from_hour", default=FROM_HOUR_DEF,
                      help="for each found day start listing from hour - inclusive -"
                           " fomat: HH defaults to: " + FROM_HOUR_DEF)
    parser.add_option("-t", "--to_hour", dest="to_hour", default=TO_HOUR_DEF,
                      help="for each found day end listing at to_hour- inclusive -"
                           " fomat: HH defaults to: " + TO_HOUR_DEF)
    parser.add_option("-p", "--logs_path", dest="logs_path", default=BRO_LOGS_PATH,
                      help="base path for day-hour indexed logs - defaults to: " + BRO_LOGS_PATH)

    (options, args) = parser.parse_args()
    if not options.start_date \
            or not options.mode \
            or not options.output_path:
        parser.print_help()
        parser.error('options not given')
    try:
        effective_end_date, effective_start_date = parse_dates(options)
    except ValueError as e:
        parser.print_help()
        parser.error(e.message)
    return effective_end_date, effective_start_date, options


def main():
    effective_end_date, effective_start_date, options = prepare_opt_parser()
    show_msg(effective_end_date, effective_start_date)

    lister = FileLister(
        effective_start_date,
        effective_end_date,
        path=options.logs_path,
        name_wildcard=BRO_LOG_NAME_PATTERN,
        from_hour=options.from_hour,
        to_hour=options.to_hour
    )

    reader = BroLogReader()

    try:
        writer = OPERATIONS[options.mode](lister, reader, options.output_path)
        writer.proceed()
    except KeyError as e:
        print str(e) + " no such output module"


if __name__ == "__main__":
    # ./serializer.py -d 2016-06-19 -f 00 -t 23 -p /share/http_log/ -m hdfs -o /tmp
    main()
