import pytest
import sys
import shutil
import glob
import numpy as np

sys.path.append('../../../fire_recovery')



class TestBaselayersMerge:
    '''Still planning -- I think the ROI paths will just be defined in the test_get_baselayers_config.yml, 
    and run_test_workflow will be run over all ROIS (I'll specify this in the test snakefile based on the test config) 
    
    OTHER OPTION: for each merged layer, extract sample points across CA for all expected years'''
    @pytest.fixture(scope='session')
    # def ul_testroi_wgs84(self):
    #     """Read in test ROI -- upper left CA, wgs84"""
    #     return gpd.read_file('data/test_data/test_ROIS/UL_CA_wgs84_testROI.shp')
    # @pytest.fixture(scope='session')
    # def ul_testroi_conusalbers(self):
    #     """Read in test ROI -- upper left CA, conus albers"""
    #     return gpd.read_file('data/test_data/test_ROIS/UL_CA_conusalbers_testROI.shp')
    # @pytest.fixture(scope='session')
    # def lr_testroi_wgs84(self):
    #     """Read in test ROI -- lower right CA, wgs84"""
    #     return gpd.read_file('data/test_data/test_ROIS/LR_CA_wgs84_testROI.shp')
    # @pytest.fixture(scope='session')
    # def lr_testroi_conusalbers(self):
    #     """Read in test ROI -- lower right CA, conus albers"""
    #     return gpd.read_file('data/test_data/test_ROIS/LR_CA_conusalbers_testROI.shp')
    @pytest.fixture(scope="session")
    def landfire_truth_samplepts():
        # Clip merged baselayers outputs from full Snakefile workflow to tiny ROIs

        # Extract sample points from merged baselayers output from full Snakefile workflow
        # TODO

        # save sampled points and clipped ROIs to data/test_data/test_baselayers/ making dir if not exist

        yield
        
        # # TEARDOWN: Remove clipped data after all tests using this fixture complete
        # print("Removing clipped test output...")
        # shutil.rmtree("data/test_data/test_baselayers") # should just remove shp of values extracted from merged layers, not truth values

    @pytest.fixture(scope="session")
    def landfire_merged_samplepts():
        # Clip merged baselayers outputs from full Snakefile workflow to tiny ROIs

        # Extract sample points from merged baselayers output from full Snakefile workflow
        # TODO

        # save sampled points and clipped ROIs to data/test_data/test_baselayers/ making dir if not exist

        yield
        
        # # TEARDOWN: Remove clipped data after all tests using this fixture complete
        # print("Removing clipped test output...")
        # shutil.rmtree("data/test_data/test_baselayers") # should just remove shp of values extracted from merged layers, not truth values

    @pytest.fixture(scope="session")
    def mtbs_sev_truth_samplepts():
        # Clip merged baselayers outputs from full Snakefile workflow to tiny ROIs

        # Extract sample points from merged baselayers output from full Snakefile workflow
        #  (use some gdal functionality to do this quickly?)
        # TODO

        # save sampled points and clipped ROIs to data/test_data/test_baselayers/ making dir if not exist

        yield
        
        # # TEARDOWN: Remove clipped data after all tests using this fixture complete
        # print("Removing clipped test output...")
        # shutil.rmtree("data/test_data/test_baselayers")

    @pytest.fixture(scope="session")
    def mtbs_sev_merged_samplepts():
        # Clip merged baselayers outputs from full Snakefile workflow to tiny ROIs

        # Extract sample points from merged baselayers output from full Snakefile workflow
        #  (use some gdal functionality to do this quickly?)
        # TODO

        # save sampled points and clipped ROIs to data/test_data/test_baselayers/ making dir if not exist

        yield
        
        # # TEARDOWN: Remove clipped data after all tests using this fixture complete
        # print("Removing clipped test output...")
        # shutil.rmtree("data/test_data/test_baselayers")

    def test_dtype(self):
        """For each merged baselayer, check that the dtype is correct for each layer using gdalinfo"""
        # For netcdf


    def check_crs(self):
        """For each merged baselayer, check it has a CRS using gdalinfo"""


    def check_disturbance_values(self, landfire_merged_samplepts):
        # Pulling from the smaller clipped test files, check sampled disturbance values for all years
        print('Check dist not implemented')
        assert True


    def check_asp_values(self, landfire_merged_samplepts):
        # Pulling from the smaller clipped test files, check sampled aspect values
        print('Check asp not implemented')
        assert True


    def check_elev_values(self, landfire_merged_samplepts):
        # Pulling from the smaller clipped test files, check sampled elevation values
        print('Check elev not implemented')
        assert True


    def check_slope_values(self, landfire_merged_samplepts):
        # Pulling from the smaller clipped test files, check sampled slope values
        print('Check slope not implemented')
        assert True


    def check_mtbs_sev(self, mtbs_sev_merged_samplepts):
        # Check that we have MTBS severity data for all years that matches sampled point values
        print('Check mtbs sev not implemented')
        assert True

        
    def check_mtbs_poly_dir(self, mtbs_poly_dir, yrly_firecount_expected):
        # Count the number of unique fire IDs per year, check that it matches what I manually check on MTBS website
        for year_dir in glob.glob(mtbs_poly_dir):
            yr = int(year_dir.split('/')[-1])
            shp_files = glob.glob(f'{year_dir}/*_burn_bndy.shp')
            fire_ids_unique = np.unique([
                os.path.basename(f).split('_')[0] for f in shp_files
            ])
            fire_count = len(fire_ids_unique)

            if fire_count != yrly_firecount_expected[yr]:
                fire_count_mismatch[yr] = {
                    'observed': fire_count,
                    'expected': yrly_firecount_expected[yr]
                }
                print(f'MTBS POLY DIR FOR YEAR {yr} DOES NOT CONTAIN EXPECTED COUNT OF POLYGONS.\n Observed fires: \t {fire_count} \n Expected fires: \t {yrly_firecount_expected[yr]}')
        
        assert len(fire_count_mismatch.keys())==0

def check_mtbs_poly_merged(self, mtbs_poly_merged, yrly_firecount_expected):
        # Count the number of unique fire IDs per year, check that it matches what I manually check on MTBS website