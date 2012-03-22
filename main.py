#
# Copyright 2012 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import time
import logging

from google.appengine.api import app_identity
from google.appengine.api import memcache
from google.appengine.api import channel
from google.appengine.api import users

import httplib2
import webapp2
from apiclient.discovery import build
from oauth2client.appengine import AppAssertionCredentials
from mapreduce import base_handler, mapreduce_pipeline
from mapreduce.lib import pipeline

import jinja2
jinja_environment = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)))

credentials = AppAssertionCredentials(
    scope='https://www.googleapis.com/auth/bigquery')

bqproject = '34784107592'
bqdataset = 'logs'
gsbucketname = 'log2bq_logs'

http = credentials.authorize(httplib2.Http(memcache))
service = build('bigquery','v2',http=http)

def message(template, *args, **kwargs):
    message = jinja2.Template(template).render(**kwargs)
    logging.debug(message)
    client_id = memcache.get('client_id')
    channel.send_message(client_id, message)

class Log2Bq(base_handler.PipelineBase):
  """A pipeline to ingest log as CSV in Google Big Query
  """

  def run(self, start_time, end_time, version_ids):
    message('ae://logs?version={{ version }}&start={{ start }}&end={{ end }} > bq://{{ dataset }} <a href="{{ pipeline }}">pipeline</a> started',
            version=version_ids[0],
            start=start_time,
            end=end_time,
            dataset=bqdataset,
            pipeline="%s/status?root=%s" % (self.base_path , self.pipeline_id))
    files = yield Log2Gs(start_time, end_time, version_ids)
    date = time.strftime("%Y%m%d", time.localtime(start_time))
    yield Gs2Bq(date, files)

class Log2Gs(base_handler.PipelineBase):
  """A pipeline to ingest log as CSV in Google Storage
  """

  def run(self, start_time, end_time, version_ids):
    message('ae://logs?version={{ version }}&start={{ start }}&end={{ end }} > gs://{{ bucket }}',
            version=version_ids[0],
            start=start_time,
            end=end_time,
            bucket=gsbucketname)
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
            "gs_bucket_name": gsbucketname
            },
        shards=16)

def log2csv(l):
  """Convert log API RequestLog object to csv."""
  message('MapperPipeline.log2csv')
  yield '%s,%s,%s,%s,%s,%s,"%s"\n' % (l.start_time, l.method, l.resource,
                                      l.status, l.latency, l.response_size,
                                      l.user_agent)

class Gs2Bq(base_handler.PipelineBase):
  """A pipeline to injest log csv from Google Storage to Google  Big Query.
  """

  def run(self, date, files):
    jobs = service.jobs()
    table = 'requestlogs_%s' % date
    gspaths = [f.replace('/gs/', 'gs://') for f in files]
    result = jobs.insert(projectId=bqproject,
                         body=jobData(table, gspaths)).execute()
    message('{{ gs }} > bq://{{ dataset }}/{{ table }} [bq://jobs/{{ job }}]',
            gs=gspaths[0],
            dataset=bqdataset,
            table=table,
            job=result['jobReference']['jobId'])
    yield BqCheck(result['jobReference']['jobId'])

class BqCheck(base_handler.PipelineBase):
  def run(self, job):
    jobs = service.jobs()
    status = jobs.get(projectId=bqproject,
                      jobId=job).execute()

    message('[bq://jobs/{{ job }}]: {{ status }}',
            job=job,
            status=status['status']['state'])

    if status['status']['state'] == 'PENDING' or status['status']['state'] == 'RUNNING':
      delay = yield pipeline.common.Delay(seconds=1)
      with pipeline.After(delay):
        yield BqCheck(job)
    else:
      yield pipeline.common.Return(status)

class MainHandler(webapp2.RequestHandler):
  def get(self):
    user = users.get_current_user()
    if not user:
      self.redirect(users.create_login_url(self.request.uri))
      return

    client_id = "%s" % user.user_id()
    memcache.set('client_id', client_id)

    channel_token = channel.create_channel(client_id)
    template_values = { 'channel_token': channel_token }
    
    template = jinja_environment.get_template('index.html')
    self.response.out.write(template.render(template_values))

class StartHandler(webapp2.RequestHandler):
  def post(self):
    # TODO(proppy): add form/ui for start_time and end_time parameter
    now = time.time()
    yesterday = now - 3600 * 24
    major, minor = os.environ["CURRENT_VERSION_ID"].split(".")
    p = Log2Bq(yesterday, now, [major])
    p.start()

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
                  'writeDisposition':'WRITE_APPEND',
                  'encoding':'UTF-8'
                  }
              }
          }

app = webapp2.WSGIApplication([('/', MainHandler),
                               ('/start', StartHandler)],
                               debug=True)
