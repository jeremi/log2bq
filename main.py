# Copyright 2011 Google Inc. All Rights Reserved.

import os
import sys
import time
import logging

from google.appengine.api import app_identity
from google.appengine.api import memcache

import httplib2
import webapp2
from apiclient.discovery import build
from oauth2client.appengine import AppAssertionCredentials
from mapreduce import base_handler, mapreduce_pipeline

credentials = AppAssertionCredentials(
    scope='https://www.googleapis.com/auth/bigquery')

bqproject = '814690009706'
bqdataset = 'appenginelogs'

http = credentials.authorize(httplib2.Http(memcache))
service = build('bigquery','v2',http=http)

class Log2Bq(base_handler.PipelineBase):
  """A pipeline to ingest log as CSV in Google Big Query
  """

  def run(self, start_time, end_time, version_ids):
    output = yield Log2Gs(start_time, end_time, version_ids)
    date = time.strftime("%Y%m%d", time.localtime(start_time))
    yield Gs2Bq(date, output)

class Log2Gs(base_handler.PipelineBase):
  """A pipeline to ingest log as CSV in Google Storage
  """

  def run(self, start_time, end_time, version_ids):

    yield mapreduce_pipeline.MapperPipeline(
        "log2bq",
        "main.log2csv",
        "mapreduce.input_readers.LogInputReader",
        output_writer_spec="mapreduce.output_writers.FileOutputWriter",
        params={
            "start_time": start_time,
            "end_time": end_time,
            "version_ids": version_ids,
            "filesystem": "gs",
            "gs_bucket_name": "appenginelogs",
            "gs_acl": "project-private"
            },
        shards=16)

class Gs2Bq(base_handler.PipelineBase):
  """A pipeline to injest log csv from Google Storage to Google  Big Query.
  """

  def run(self, date, output):
    logging.debug("output is %s" % str(output))
    jobs = service.jobs()
    table = 'requestlogs_%s' % date
    gspaths = [s.replace('/gs/', 'gs://') for s in output]
    result = jobs.insert(projectId=bqproject,
                         body=jobData(table, gspaths)).execute()
    logging.debug("bigquery ingestion result is %s" % str(result))
    return result

def log2csv(l):
  """Convert log API RequestLog object to csv."""
  yield '%s,%s,%s,%s,%s,%s,"%s"\n' % (l.start_time, l.method, l.resource,
                                      l.status, l.latency, l.response_size,
                                      l.user_agent)

class StartHandler(webapp2.RequestHandler):
  def get(self):
    # TODO(proppy): add form/ui for start_time and end_time parameter
    now = time.time()
    yesterday = now - 3600 * 24
    major, minor = os.environ["CURRENT_VERSION_ID"].split(".")
    pipeline = Log2Bq(yesterday, now, [major])
    pipeline.start()
    self.redirect(pipeline.base_path + "/status?root=" + pipeline.pipeline_id)

def jobData(tableId, sourceUris):
  return {'projectId': bqproject,
          'configuration':{
              'load':{
                  'sourceUris': sourceUris,
                  'schema':{
                      'fields':[
                          {
                              'name':'start_time',
                              'type':'FLOAT',
                              'mode':'REQUIRED',
                              },
                          {
                              'name':'method',
                              'type':'STRING',
                              'mode':'REQUIRED',
                              },
                          {
                              'name':'resource',
                              'type':'STRING',
                              'mode':'REQUIRED',
                              },
                          {
                              'name':'status',
                              'type':'INTEGER',
                              'mode':'REQUIRED',
                              },
                          {
                              'name':'latency',
                              'type':'FLOAT',
                             'mode':'REQUIRED',
                              },
                          {
                              'name':'response_size',
                              'type':'INTEGER',
                              'mode':'REQUIRED',
                              },
                          {
                              'name':'user_agent',
                              'type':'STRING'
                              }
                          ]
                      },
                  'destinationTable':{
                     'projectId': bqproject,
                     'datasetId': bqdataset,
                     'tableId': tableId
                     },
                  'createDisposition':'CREATE_IF_NEEDED',
                  'writeDisposition':'WRITE_TRUNCATE',
                  'encoding':'UTF-8'
                  }
              }
          }

app = webapp2.WSGIApplication([('/',StartHandler)],debug=True)
