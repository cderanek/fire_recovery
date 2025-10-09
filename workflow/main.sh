#### submit_job.sh START ####
#!/bin/bash
#$ -cwd


# request resources:
#$ -l h_rt=336:00:00,h_data=10G,highp

#$ -t 1-3                       # task IDs (breaking the full snakefile workflow into 3 checkpoints)
#$ -tc 1                        # maximum concurrent jobs = 1 (run sequentially)
# error = Merged with joblog
#$ -o main_snakefile_joblog.$JOB_ID.$TASK_ID
#$ -j y

# Load anaconda to access snakefile
. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate RIO_GPD

# add access to qsub commands
export PATH=/u/systems/UGE8.6.4/bin/lx-amd64:$PATH


# checkpoint 1: create baselayers + sh script wrapper to coordinate appeears downloads
if [ $SGE_TASK_ID -eq 1 ]; then
    # checkpoint 1
    snakemake --rulegraph | dot -Tpng > docs/images/rulegraph.png
    snakemake coordinate_appeears_requests --rulegraph | dot -Tpng > docs/images/rulegraph_baselayers.png
    snakemake --profile profiles/age get_baselayers
    snakemake --dag | dot -Tpng > docs/images/baselayers_dag.png
fi

# checkpoint 2: start running coordinate_appears_tasks job + create recovery maps
if [ $SGE_TASK_ID -eq 2 ]; then
    # checkpoint 2
    snakemake allfire_recovery --rulegraph | dot -Tpng > docs/images/rulegraph_allfires.png

    # submit separate, small job to coordinate appeears downloads 
    qsub workflow/calculate_recovery/sh_scripts/coordinate_appeears_requests_wrapper.sh
    sleep 21600 # sleep 6 hours to allow time for the jobs to be proccessed on appeears

    # keep checking back for new fires that are ready to download/process until the job is complete
    # cap this at 200 loops in 2 weeks
    for i in {1..200}; do
        echo "=== Fire check $i at $(date) ==="
        # Try to resubmit snake to look for new fires to process
        # won't run if previous job is still running (bc lock on snake)
        snakemake --profile profiles/age \
            --rerun-incomplete \
            --quiet \
            allfire_recovery
        
        # If snakemake exits successfully and found nothing to do, we're done
        if snakemake --profile profiles/age --dryrun --quiet allfire_recovery 2>&1 | grep -q "Nothing to be done"; then
            echo "=== All fires processed! Exiting early at $(date) ==="
            exit 0
        fi
    done
fi

# Task 3: checkpoints 3 and 4
# checkpoint 3: repeat checkpoint 2 to ensure all fires done
# checkpoint 4: merge recovery maps and run analyses (run to end of workflow)
if [ $SGE_TASK_ID -eq 3 ]; then
    # checkpoint 3 (same as checkpoint 2)
    # submit separate, small job to coordinate appeears downloads 
    qsub workflow/calculate_recovery/sh_scripts/coordinate_appeears_requests_wrapper.sh

    # keep checking back for new fires that are ready to download/process until the job is complete
    # cap this at 200 loops in 2 weeks
    for i in {1..200}; do
        echo "=== Fire check $i at $(date) ==="
        # Try to resubmit snake to look for new fires to process
        # won't run if previous job is still running (bc lock on snake)
        snakemake --profile profiles/age \
            --rerun-incomplete \
            --quiet \
            allfire_recovery
        
        # If snakemake exits successfully and found nothing to do, we're done
        if snakemake --profile profiles/age --dryrun --quiet allfire_recovery 2>&1 | grep -q "Nothing to be done"; then
            echo "=== All fires processed! Exiting early at $(date) ==="
            break  # exit the loop, continue to checkpoint 4
        fi
    done

    # checkpoint 4
    snakemake --rulegraph | dot -Tpng > docs/images/rulegraph_full.png
    snakemake --profile profiles/age
fi