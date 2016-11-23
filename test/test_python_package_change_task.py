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

import mock
import os
import unittest

from pipeline.tasks import protoc_tasks


class PythonPackageChangeTest(unittest.TestCase):

    _TASK = protoc_tasks.PythonChangePackageTask()

    _PROTO_FILE = [
        '# Comment line\n',
        'package google.service.v1;\n',
        'import "google/service/a.proto";\n',
        'import "google/cloud/otherapi/b.proto";\n',
        'import "google/common/common_proto.proto";\n',
        'Some other text referencing to google.service.v1\n']

    def test__extract_base_dirs(self):
        mock_proto = mock.mock_open()
        mock_proto.return_value.__iter__ = lambda _: iter(self._PROTO_FILE)
        with mock.patch('__builtin__.open', mock_proto, create=True):
            base_dirs = self._TASK._extract_base_dirs(
                os.path.join('a', 'test', 'path', 'to', 'google', 'service',
                             'v1', 'a.proto'))
        self.assertEqual(base_dirs, os.path.join('google', 'service', 'v1'))

    def test__transfom(self):
        # Simple package transformations with arbitrary separator
        self.assertEqual(self._TASK._transform('google.service', '.', []),
                         'google.cloud.grpc.service')
        self.assertEqual(self._TASK._transform('google/other', '/', []),
                         'google/cloud/grpc/other')
        self.assertEqual(self._TASK._transform('google$service', '$', []),
                         'google$cloud$grpc$service')

        # Don't transform common protos
        self.assertEqual(
            self._TASK._transform('google/common', '/', ['google.common']),
            'google/common')
        self.assertEqual(
            self._TASK._transform('google/uncommon', '/', ['google.service']),
            'google/cloud/grpc/uncommon')

        # Don't transform non-Google protos
        self.assertEqual(
            self._TASK._transform('my_custom/path', '/', ['']),
            'my_custom/path')

    def test__copy_proto(self):
        mock_proto = mock.mock_open()
        mock_proto.return_value.__iter__ = lambda _: iter(self._PROTO_FILE)
        with mock.patch('__builtin__.open', mock_proto, create=True):
            self._TASK._copy_proto('foo', 'bar', ['google.common'])
        expected_writes = [
            mock.call('# Comment line\n'),
            mock.call('package google.service.v1;\n'),
            mock.call('import "google/cloud/grpc/service/a.proto";\n'),
            mock.call('import "google/cloud/grpc/otherapi/b.proto";\n'),
            mock.call('import "google/common/common_proto.proto";\n'),
            mock.call('Some other text referencing to google.service.v1\n')]

        mock_proto().write.assert_has_calls(expected_writes)
