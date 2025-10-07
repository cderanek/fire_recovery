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

# checkpoint 1: create baselayers + create recovery maps for all the sensitivity analysis fires
if [ $SGE_TASK_ID -eq 1 ]; then
    snakemake --rulegraph | dot -Tpng > docs/images/rulegraph.png
    snakemake --profile profiles/age sensitivity_fires_done
fi

# checkpoint 2: create recovery maps for all remaining fires
if [ $SGE_TASK_ID -eq 2 ]; then
    snakemake --profile profiles/age allfire_recovery
fi

# checkpoint 3: merge recovery maps and run analyses (run to end of workflow)
if [ $SGE_TASK_ID -eq 3 ]; then
    snakemake --profile profiles/age
fi