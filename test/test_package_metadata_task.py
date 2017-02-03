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

import os
import pytest
import unittest
import yaml

from pipeline.tasks import package_metadata_tasks


class PackageMetadataConfigTest(unittest.TestCase):
    # Print full diff when comparing actual/expected dict output
    maxDiff = None

    @pytest.fixture(autouse=True)
    def outdir(self, tmpdir):
        self.output_dir = tmpdir.mkdir('out')

    def test_package_metadata_config_gen_task(self):
        task = package_metadata_tasks.PackageMetadataConfigGenTask()
        repo_root = os.path.abspath('.')

        package_dependencies_yaml = os.path.join(
            repo_root,
            'test/testdata/googleapis_test/gapic/packaging/dependencies.yaml')
        package_defaults_yaml = os.path.join(
            repo_root,
            'test/testdata/googleapis_test/gapic/packaging/api_defaults.yaml')

        task.execute(api_name='fake', api_version='v1',
                     organization_name='google-cloud',
                     output_dir=str(self.output_dir),
                     package_dependencies_yaml=package_dependencies_yaml,
                     package_defaults_yaml=package_defaults_yaml,
                     proto_gen_pkg_deps=['googleapis-common-protos'],
                     repo_root=repo_root,
                     src_proto_path=['path/to/protos'])
        with open(os.path.join(str(self.output_dir),
                               'google-cloud-fake-v1_package.yaml')) as f:
            actual = yaml.load(f)
        with open('test/testdata/google-cloud-fake-v1_package.yaml') as f:
            expected = yaml.load(f)
        # Don't compare files directly because yaml doesn't preserve ordering
        self.assertDictEqual(actual, expected)
