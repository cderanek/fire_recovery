#!/bin/bash
#$ -cwd
#$ -o logs/california/calculate_recovery/coordinate_appeears_requests.cluster.out
#$ -e logs/california/calculate_recovery/coordinate_appeears_requests.cluster.err
#$ -j y
#$ -l h_rt=336:00:00,h_data=10G,highp
#$ -pe shared 1
#$ -M caderanek@g.ucla.edu
#$ -m bea

# Execute the appeeears download coordinator script
workflow/calculate_recovery/sh_scripts/coordinate_appeears_requests.sh \
    EARTHACCESS \
    data/california/recovery_maps/submission_organizer/main_config.json \
    data/california/recovery_maps/submission_organizer/perfire_config.json \
    logs/california/calculate_recovery/done/ready_to_download_fireid.done \
    workflow/calculate_recovery/sh_scripts/coordinate_appeears_requests_wrapper.sh > logs/california/calculate_recovery/coordinate_appeears_requests_setup.log 2> logs/california/calculate_recovery/coordinate_appeears_requests_setup.err
