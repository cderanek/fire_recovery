#!/bin/bash
#$ -cwd
#$ -o logs/UL_CA_wgs84_testROI/calculate_recovery/coordinate_appeears_requests.cluster.out
#$ -e logs/UL_CA_wgs84_testROI/calculate_recovery/coordinate_appeears_requests.cluster.err
#$ -j y
#$ -l h_rt=336:00:00,h_data=10G,highp
#$ -pe shared 1
#$ -M caderanek@g.ucla.edu
#$ -m bea

# Execute the appeeears download coordinator script
workflow/calculate_recovery/sh_scripts/coordinate_appeears_requests.sh \
    EARTHACCESS \
    data/UL_CA_wgs84_testROI/recovery_maps/submission_organizer/main_config.json \
    data/UL_CA_wgs84_testROI/recovery_maps/submission_organizer/perfire_config.json \
    logs/UL_CA_wgs84_testROI/calculate_recovery/done/ready_to_download_fireid.done \
    workflow/calculate_recovery/sh_scripts/coordinate_appeears_requests_wrapper.sh > logs/UL_CA_wgs84_testROI/calculate_recovery/coordinate_appeears_requests_setup.log 2> logs/UL_CA_wgs84_testROI/calculate_recovery/coordinate_appeears_requests_setup.err
