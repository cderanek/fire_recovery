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

# Task 1: checkpoints 1 and 2
# checkpoint 1: create baselayers
# checkpoint 2: start running coordinate_appears_tasks job + create recovery maps
if [ $SGE_TASK_ID -eq 1 ]; then
    # checkpoint 1
    snakemake --rulegraph | dot -Tpng > docs/images/rulegraph.png
    snakemake get_baselayers --rulegraph | dot -Tpng > docs/images/rulegraph_baselayers.png
    snakemake --profile profiles/age get_baselayers

    # checkpoint 2
    snakemake allfire_recovery --rulegraph | dot -Tpng > docs/images/rulegraph_allfires.png
    snakemake --profile profiles/age allfire_recovery
fi

# checkpoint 3: repeat checkpoint 2 to ensure all fires done
if [ $SGE_TASK_ID -eq 2 ]; then
    snakemake allfire_recovery --rulegraph | dot -Tpng > docs/images/rulegraph_allfires.png
    snakemake --profile profiles/age allfire_recovery
fi

# checkpoint 4: merge recovery maps and run analyses (run to end of workflow)
if [ $SGE_TASK_ID -eq 3 ]; then
    snakemake --rulegraph | dot -Tpng > docs/images/rulegraph_checkpoint3.png
    snakemake --profile profiles/age
fi