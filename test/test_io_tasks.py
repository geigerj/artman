import unittest

from pipeline.tasks import io_tasks


_UPLOAD_LIMIT = 123


class ValidateUploadSizeTest(unittest.TestCase):

    def test_validate_upload_size_ok(self):
        io_tasks._validate_upload_size(_UPLOAD_LIMIT, _UPLOAD_LIMIT)

    def test_validate_upload_size_bad(self):
        self.assertRaises(
            ValueError, io_tasks._validate_upload_size,
            _UPLOAD_LIMIT + 1, _UPLOAD_LIMIT)
