from collections import defaultdict
import inspect
from io import BytesIO
import json
import os
import copy
import decimal
import statsrunner
import datetime

import boto3
import requests
from rq import Queue

from statsrunner import common
from worker import conn


def decimal_default(obj):
    if hasattr(obj, 'value'):
        if type(obj.value) == datetime.datetime:
            return obj.value.strftime('%Y-%m-%d %H:%M:%S %z')
        else:
            return obj.value
    else:
        return common.decimal_default(obj)


def dict_sum_inplace(d1, d2):
    """Merge values from dictionary d2 into d1."""
    if d1 is None:
        return
    for k, v in d2.items():
        if type(v) == dict or type(v) == defaultdict:
            if k in d1:
                dict_sum_inplace(d1[k], v)
            else:
                d1[k] = copy.deepcopy(v)
        elif (type(d1) != defaultdict and not k in d1):
            d1[k] = copy.deepcopy(v)
        elif d1[k] is None:
            continue
        else:
            d1[k] += v


def make_blank(stats_module):
    """Return dictionary of stats functions for enabled stats_modules."""
    blank = {}
    for stats_object in [stats_module.ActivityStats(),
                         stats_module.ActivityFileStats(),
                         stats_module.OrganisationStats(),
                         stats_module.OrganisationFileStats(),
                         stats_module.PublisherStats(),
                         stats_module.AllDataStats()]:
        stats_object.blank = True
        for name, function in inspect.getmembers(stats_object, predicate=inspect.ismethod):
            if not statsrunner.shared.use_stat(stats_object, name):
                continue
            blank[name] = function()
    return blank


def aggregate_file(stats_module, stats_json, output_dir):
    """Create JSON file for each stats_module function."""
    subtotal = make_blank(stats_module)  # FIXME This may be inefficient
    for activity_json in stats_json['elements']:
        dict_sum_inplace(subtotal, activity_json)
    dict_sum_inplace(subtotal, stats_json['file'])

    for aggregate_name, aggregate in subtotal.items():
        filepath = os.path.join(output_dir, aggregate_name+'.json')
        data = BytesIO(json.dumps(
            aggregate, sort_keys=True,
            indent=2, default=decimal_default).encode('utf-8'))
        save_json_file(data, filepath)
    return subtotal


def save_json_file(data, filepath):
    s3 = boto3.client('s3')
    bucket_name = os.getenv('S3_BUCKET_NAME')
    s3.upload_fileobj(
        data,
        bucket_name,
        filepath,
        ExtraArgs={'ACL': 'public-read',
                   'ContentType': 'application/json'})


def list_bucket(prefix):
    s3 = boto3.client('s3')
    bucket_name = os.getenv('S3_BUCKET_NAME')
    start_after = ''
    all_files = []
    max_keys = 1000
    while True:
        data = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            Delimiter='/',
            MaxKeys=max_keys,
            StartAfter=start_after)
        folders = [x['Prefix'] for x in data.get('CommonPrefixes', [])]
        files = [x['Key'] for x in data.get('Contents', [])]
        all_files += folders + files
        if len(all_files) % max_keys != 0:
            break
        start_after = folders[-1]
    return all_files


def run_aggregate(args):
    import importlib
    stats_module = importlib.import_module(args.stats_module)

    # for newdir in ['aggregated-publisher', 'aggregated-file', 'aggregated']:
    #     try:
    #         os.mkdir(os.path.join(args.output, newdir))
    #     except OSError:
    #         pass

    blank = make_blank(stats_module)

    if args.verbose_loop:
        base_folder = os.path.join(args.output, 'loop')
    else:
        base_folder = os.path.join(args.output, 'aggregated-file')
    total = copy.deepcopy(blank)
    for path in list_bucket(base_folder + '/'):
        folder = path[:-1].rsplit('/', 1)[-1]
        publisher_total = copy.deepcopy(blank)

        for jsonfilefolder in list_bucket(path):
            # if args.verbose_loop:
            #     with open(os.path.join(base_folder, folder, jsonfilefolder)) as jsonfp:
            #         stats_json = json.load(jsonfp, parse_float=decimal.Decimal)
            #         subtotal = aggregate_file(stats_module,
            #                                   stats_json,
            #                                   os.path.join(args.output,
            #                                                'aggregated-file',
            #                                                folder,
            #                                                jsonfilefolder))
            # else:
            subtotal = copy.deepcopy(blank)
            for jsonfile in list_bucket(jsonfilefolder):
                url = 'http://stats.codeforiati.org/' + jsonfile
                r = requests.get(url)
                stats_json = json.load(BytesIO(r.content), parse_float=decimal.Decimal)
                subtotal[jsonfile.rsplit('/', 1)[-1][:-5]] = stats_json

            dict_sum_inplace(publisher_total, subtotal)

        publisher_stats = stats_module.PublisherStats()
        publisher_stats.aggregated = publisher_total
        publisher_stats.folder = folder
        publisher_stats.today = args.today
        for name, function in inspect.getmembers(publisher_stats, predicate=inspect.ismethod):
            if not statsrunner.shared.use_stat(publisher_stats, name):
                continue
            publisher_total[name] = function()

        dict_sum_inplace(total, publisher_total)
        for aggregate_name, aggregate in publisher_total.items():
            # try:
            #     os.mkdir(os.path.join(args.output, 'aggregated-publisher', folder))
            # except OSError:
            #     pass
            filepath = os.path.join(args.output,
                                    'aggregated-publisher',
                                    folder,
                                    aggregate_name+'.json')
            data = BytesIO(json.dumps(
                aggregate, sort_keys=True,
                indent=2, default=decimal_default).encode('utf-8'))
            save_json_file(data, filepath)

    all_stats = stats_module.AllDataStats()
    all_stats.aggregated = total
    for name, function in inspect.getmembers(all_stats, predicate=inspect.ismethod):
        if not statsrunner.shared.use_stat(all_stats, name):
            continue
        total[name] = function()

    for aggregate_name, aggregate in total.items():
        filepath = os.path.join(args.output,
                                'aggregated',
                                aggregate_name+'.json')
        data = BytesIO(json.dumps(
            aggregate, sort_keys=True,
            indent=2, default=decimal_default).encode('utf-8'))
        save_json_file(data, filepath)


def aggregate(args):
    q = Queue(connection=conn, default_timeout=600)
    q.enqueue(run_aggregate,
              args,
              result_ttl=0)
