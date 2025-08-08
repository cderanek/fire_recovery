import pytest
import sys

# Import your functions (adjust import path as needed)
sys.path.append('/u/project/eordway/shared/surp_cd/fire_recovery')
from workflow.get_baselayers.download_clip_landfire import *


class TestLandfireDownload:
    def test_checksum(self):
        # For each product in the LANDFIRE_PRODUCTS config list, test the checksum matches
        self.value = 1
        assert self.value == 1

    def test_metadata_exist(self):
        assert self.value == 1

    def test_unzip_success(self):
        # For each product in the LANDFIRE_PRODUCTS config list, check that all expected years of data downloaded

class TestLandfireClip:
    def test_extent(self):
        # For each unzipped/clipped product, check that the extent matches the ROI

    def test_dtype(self):
        # For each unzipped/clipped product, check that the dtype and rxr type are correct
        
    def check_crs(self):
        # For each unzipped/clipped product, check that it has a CRS using gdalinfo

    def check_disturbance_values(self):
        # Pulling from the smaller clipped test files, check a few sampled disturbance values

    def check_topo_values(self):
        # Pulling from the smaller clipped test files, check a few sampled topo values