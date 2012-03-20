#!/usr/bin/python2.4
#
# Copyright 2010 Google Inc. All Rights Reserved.

"""A horrible hack that the author should be ashamed of.

There is no other way to fix the imports before loading the mapreduce.main
module that to wrap it and fix the imports before the actual loading of the
module.
"""

__author__ = 'ttrimble@google.com (Troy Trimble)'

import auto_import_fixer

from mapreduce import main

if __name__ == '__main__':
  main.main()
