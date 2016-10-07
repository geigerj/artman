#!/usr/bin/env python

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


"""Main class to start jobboard cleaner."""

import argparse

from pipeline.jobboard import jobboard_consumer


TWELVE_HOURS_IN_SECONDS = 60 * 60 * 12


def main():
  jobboard_name, time_to_live = _parse_args()
  cleaner = jobboard_consumer.cleaner(time_to_live)
  cleaner(jobboard_name)

def _parse_args():
  parser = _CreateArgumentParser()
  flags = parser.parse_args()
  return flags.jobboard_name.lower(), flags.time_to_live

def _CreateArgumentParser():
  parser = argparse.ArgumentParser()
  parser.add_argument(
      "-t", "--time_to_live",
      type=int,
      default=TWELVE_HOURS_IN_SECONDS,
      help="The time delta, in seconds, after creation when jobs should be "
           "trashed.")
  parser.add_argument(
      "--jobboard_name",
      type=str,
      required=True,
      help="The name of the jobboard to monitor.")
  return parser

if __name__ == '__main__':
  main()
