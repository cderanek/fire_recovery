import pytest
import sys

sys.path.append('../../../fire_recovery')
from workflow.get_baselayers.download_clip_landfire import *


class TestLandfireDownload:
    def test_metadata_exist(self):
        """For the full CA ROI download, confirm metadata got copied over for all products"""
        assert self.value == 1

    def test_unzip_success(self):
        """For each product in the LANDFIRE_PRODUCTS config list, check that all expected years of data downloaded"""