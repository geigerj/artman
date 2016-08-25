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

"""Tasks related to I/O."""

import base64
import os
import shutil
import urllib
import zipfile

from gcloud import storage
from pipeline.tasks import task_base


# Maximum amount of data, in bytes, that can be stored in a zookeeper node. See
# http://zookeeper.apache.org/doc/r3.1.2/api/org/apache/zookeeper/ZooKeeper.html
_ZOOKEEPER_NODE_DATA_SIZE_LIMIT = 2 ** 20


class BlobUploadTask(task_base.TaskBase):
    """A task which uploads file to Google Cloud Storage.

    It requires authentication be properly configured."""

    default_provides = ('bucket', 'path', 'public_url')

    def execute(self,
                bucket_name,
                src_path,
                dest_path):
        print "Start blob upload"
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(dest_path)
        with open(src_path, 'r') as f:
            blob.upload_from_file(f)
        print "Uploaded to %s" % blob.public_url

        return bucket_name, dest_path, blob.public_url


class BlobDownloadTask(task_base.TaskBase):
    """A task which downloads file to Google Cloud Storage.

    It requires authentication be properly configured."""

    def execute(self, bucket_name, path, output_dir):
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.get_blob(path)
        if not blob:
            print 'Cannot find the output from GCS.'
            return
        filename = os.path.join(output_dir, path)
        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except:
                raise
        with open(filename, "w") as f:
            blob.download_to_file(f)
            print 'File downloaded to %s' % f.name


class PrepareUploadDirTask(task_base.TaskBase):
    """Compress pipeline dir_to_upload into a file which can be uploaded to GCS.

    Normally be used as the final step for pipeline job to return generated
    content to its poster."""

    def execute(self, repo_root, tarfile):
        self.exec_command(['tar', '-C', repo_root, '-zcvf', tarfile, '.'])
        size = os.path.getsize(tarfile)
        if size > _ZOOKEEPER_NODE_DATA_SIZE_LIMIT:
            raise ValueError(
                'Size of merged local_repo was {}, exceeding the limit of {} '
                'bytes; reduce the size of local_repo'.format(
                    size, _ZOOKEEPER_NODE_DATA_SIZE_LIMIT))


class CleanupTempDirsTask(task_base.TaskBase):
    """Clean up all temporary directories"""

    def execute(self, repo_root):
        if os.path.exists(repo_root) and os.path.isdir(repo_root):
            shutil.rmtree(repo_root)


class PrepareGoogleapisDirTask(task_base.TaskBase):

    default_provides = ('repo_dir')

    def execute(self, repo_root, files_dict={}):
        if os.path.exists(repo_root):
            # Do nothing if the repo_root exists. The repo_root exists if
            # artman is running locally.
            return
        print('root repo: %s' % repo_root)
        try:
            os.makedirs(repo_root)
        except OSError as e:
            raise e
        testfile = urllib.FancyURLopener()
        tmp_repo_file = os.path.join(repo_root, "file.zip")
        testfile.retrieve(
            "https://github.com/googleapis/googleapis/archive/master.zip",
            tmp_repo_file)
        zip_ref = zipfile.ZipFile(tmp_repo_file, 'r')
        zip_ref.extractall(repo_root)
        zip_ref.close()
        os.remove(tmp_repo_file)
        shutil.move(os.path.join(repo_root, "googleapis-master"),
                    os.path.join(repo_root, "googleapis"))
        repo_dir = os.path.join(repo_root, "googleapis")
        # Write/overwrite the additonal files into the repo_dir so that user
        # can include additional files which are not in the public repo.
        for f, content in files_dict.iteritems():
            filename = os.path.join(repo_dir, f)
            if not os.path.exists(os.path.dirname(filename)):
                os.makedirs(os.path.dirname(filename))
            with open(filename, "w+") as text_file:
                text_file.write(base64.b64decode(content))
        return repo_dir
