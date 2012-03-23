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
import json

from google.appengine.api import app_identity
from google.appengine.api import memcache
from google.appengine.api import channel
from google.appengine.api import users

import httplib2
import webapp2
from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.appengine import AppAssertionCredentials
from mapreduce import base_handler, mapreduce_pipeline
from mapreduce.lib import pipeline
from mapreduce import context

import jinja2
jinja_environment = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)))

credentials = AppAssertionCredentials(
    scope='https://www.googleapis.com/auth/bigquery')

bqproject = '34784107592'
bqdataset = 'logs'
bqtable = 'requestlogs_%s'
gsbucketname = 'log2bq_logs'

http = credentials.authorize(httplib2.Http(memcache))
service = build('bigquery','v2',http=http)

def message(root_pipeline_id, template, *args, **kwargs):
    message = jinja2.Template(template).render(root_pipeline_id=root_pipeline_id, **kwargs)
    logging.debug(message)
    client_id = memcache.get('client_id')
    channel.send_message(client_id, "%s,%s" % (root_pipeline_id, message))

class Log2Bq(base_handler.PipelineBase):
  """A pipeline to ingest log as CSV in Google Big Query
  """

  def run(self, start_time, end_time, version_ids):
    message(self.root_pipeline_id, '<span class="label label-info">started</span> ae://logs <i class="icon-arrow-right"></i> bq://{{ dataset }} <a href="{{ base_path }}/status?root={{ root_pipeline_id }}#pipeline-{{ pipeline_id }}">pipeline</a>',
            dataset=bqdataset,
            base_path=self.base_path,
            pipeline_id=self.pipeline_id)
    files = yield Log2Gs(start_time, end_time, version_ids)
    date = time.strftime("%Y%m%d", time.localtime(start_time))
    yield Gs2Bq(date, files)

class Log2Gs(base_handler.PipelineBase):
  """A pipeline to ingest log as CSV in Google Storage
  """

  def run(self, start_time, end_time, version_ids):
    message(self.root_pipeline_id, '<span class="label label-info">started</span> ae://logs <i class="icon-arrow-right"></i> gs://{{ bucket }} <a href="{{ base_path }}/status?root={{ root_pipeline_id }}#pipeline-{{ pipeline_id }}">pipeline</a>',
            bucket=gsbucketname,
            base_path=self.base_path,
            pipeline_id=self.pipeline_id)
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
            "gs_bucket_name": gsbucketname,
            "root_pipeline_id": self.root_pipeline_id,
            },
        shards=16)

def log2csv(l):
  """Convert log API RequestLog object to csv."""
  root_pipeline_id = context.get().mapreduce_spec.mapper.params['root_pipeline_id']
  message(root_pipeline_id, '<span class="label label-warning">pending</span> MapperPipeline.log2csv')
  yield '%s,%s,%s,%s,%s,%s,"%s"\n' % (l.start_time, l.method, l.resource,
                                      l.status, l.latency, l.response_size,
                                      l.user_agent)

class Gs2Bq(base_handler.PipelineBase):
  """A pipeline to ingest log csv from Google Storage to Google BigQuery.
  """

  def run(self, date, files):
    jobs = service.jobs()
    table = bqtable % date
    gspaths = [f.replace('/gs/', 'gs://') for f in files]
    result = jobs.insert(projectId=bqproject,
                         body=jobData(table, gspaths)).execute()
    message(self.root_pipeline_id, '<span class="label label-info">started</span> {{ gs }} <i class="icon-arrow-right"></i> bq://{{ dataset }}/{{ table }} <a href="{{ base_path }}/status?root={{ root_pipeline_id }}#pipeline-{{ pipeline_id }}">pipeline</a>',
            gs=gspaths[0],
            dataset=bqdataset,
            table=table,
            base_path=self.base_path,
            pipeline_id=self.pipeline_id)
    yield BqCheck(result['jobReference']['jobId'])

class BqCheck(base_handler.PipelineBase):
  def run(self, job):
    jobs = service.jobs()
    status = jobs.get(projectId=bqproject,
                      jobId=job).execute()

    if status['status']['state'] == 'PENDING' or status['status']['state'] == 'RUNNING':
      message(self.root_pipeline_id, '<span class="label label-warning">{{ status }}</span> bq://jobs/{{ job }}',
              job=job,
              status=status['status']['state'].lower())
      delay = yield pipeline.common.Delay(seconds=1)
      with pipeline.After(delay):
        yield BqCheck(job)
    else:
      message(self.root_pipeline_id, '<span class="label label-success">{{ status }}</span> bq://jobs/{{ job }} <a href="{{ base_path }}/status?root={{ root_pipeline_id }}#pipeline-{{ pipeline_id }}">pipeline</a>',
              job=job,
              status=status['status']['state'].lower(),
              base_path=self.base_path,
              pipeline_id=self.pipeline_id)
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
    min1hour = now - 3600 * 1
    major, minor = os.environ["CURRENT_VERSION_ID"].split(".")
    p = Log2Bq(min1hour, now, [major])
    p.start()

class QueryHandler(webapp2.RequestHandler):
  def post(self):
    query = self.request.get('query')
    jobs = service.jobs()
    try:
      result = jobs.query(projectId=bqproject,
                          body={'query':query}).execute()
      self.response.out.write(json.dumps(result))
    except HttpError, e:
      self.error(500)
      self.response.out.write(e.content)

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

app = webapp2.WSGIApplication([('/', MainHandler),
                               ('/start', StartHandler),
                               ('/query', QueryHandler)],
                               debug=True)
