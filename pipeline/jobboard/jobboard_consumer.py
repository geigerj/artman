# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''Functionality for interacting with the jobboard'''

import contextlib
import datetime
import os
import time

from taskflow import exceptions as exc
from taskflow.conductors import backends as conductor_backends

from pipeline.utils import backend_helper


def _jobboard_consumer_factory(description, execute):
    def inner(jobboard_name):
        conductor_id = os.getpid()
        print('Starting GAPIC {} with pid: {}'.format(description, conductor_id))
        my_name = '{}-{}'.format(description, conductor_id)
        persist_backend = backend_helper.default_persistence_backend()
        with contextlib.closing(persist_backend):
            with contextlib.closing(persist_backend.get_connection()) as conn:
                conn.upgrade()
            jobboard = backend_helper.get_jobboard(my_name, jobboard_name)
            jobboard.connect()
            with contextlib.closing(jobboard):
                execute(jobboard, my_name, persist_backend)
    return inner


# TODO(cbao): This is now a common conductor which will execute all pipeline
# types. Turn this into an abstract class, and let its subclasses defines the
# pipelines types they can execute. Task requirements needed by pipelines shall
# be installed before conductor starts claiming jobs.
def conductor(clean_after=False):
    def execute(jobboard, my_name, persist_backend):
        cond = conductor_backends.fetch('blocking',
                                        my_name,
                                        jobboard,
                                        persistence=persist_backend,
                                        engine='serial')
        # Run forever, and kill -9 or ctrl-c me...
        try:
            print('Conductor %s is running' % my_name)
            cond.run()
        finally:
            print('Conductor %s is stopping' % my_name)
            cond.stop()
            cond.wait()
        
    return _jobboard_consumer_factory('conductor', execute)


def cleaner(time_to_live):
    def execute(jobboard, my_name, _):
        while True:
            now = datetime.datetime.now()
            for job in jobboard.iterjobs(
                    only_unclaimed=True, ensure_fresh=True):
                age = (now - job.created_on).total_seconds()
                    job, job.created_on, now)
                if (age > time_to_live):
                    try:
                        jobboard.claim(job, my_name)
                        print 'TRASH job {} from {}'.format(job, job.created_on)
                        jobboard.trash(job, my_name)
                    except exc.UnclaimableJob:
                        pass
                # Zookeeper doesn't like getting hit with too many requests at
                # once, but this seems to be a sufficient time to wait.
                time.sleep(1)
    return _jobboard_consumer_factory('cleaner', execute)
