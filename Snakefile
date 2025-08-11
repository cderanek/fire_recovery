### Define targets for the full workflow (coming soon) ###



### Download baselayers for full ROI ###
# Trigger product downloads for all baselayers
configfile: "configs/config.yml"
LANDFIRE_PRODUCTS = list(config['LANDFIRE_PRODUCTS'].keys())
rule get_baselayers:
    input:
        # LANDFIRE inputs
        expand("data/baselayers/{prod}_processed.done", prod=LANDFIRE_PRODUCTS),
        # Baselayers outputs
        # config['BASELAYERS']['groupings']['fname'],
        # config['BASELAYERS']['topo']['fname'],
        # config['BASELAYERS']['annual_dist']['fname'],
        # config['BASELAYERS']['mtbs_sev']['fname'],
        # config['BASELAYERS']['mtbs_poly']['fname']
        # still need to add RAP


# Rule for landfire download
rule get_landfire:
    input:
        ROI=config['ROI'] # Our ROI to clip to (assumed to be in CONUS)

    output: 
        ## TEMP output of downloading initial CONUS-wide files
        # LANDFIRE\
        done_flag="data/baselayers/{prod}_processed.done",

        ## DATA PROVENANCE info (date downloaded, version, metadata, etc)
        metadata_dir='data/baselayers/downloadlogs_metadata/{prod}'

        # Small output clipped from permanent output (these will be used for testing)

    log:
        "logs/get_landfire_{prod}.log"

    params:
        link=lambda wildcards: config['LANDFIRE_PRODUCTS'][wildcards.prod]['link'],
        checksum=lambda wildcards: config['LANDFIRE_PRODUCTS'][wildcards.prod]['checksum'],
        dir_name=lambda wildcards: config['LANDFIRE_PRODUCTS'][wildcards.prod]['dir_name'],
        conda_env='RIO_GPD',
        email='caderanek@g.ucla.edu'
    
    resources:
        mem_gb=30,
        runtime=24,
        cpus=1

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    shell:
        """
        qsub -cwd \
             -o {log} \
             -j y \
             -l h_rt={resources.runtime}:00:00,h_data={resources.mem_gb}G \
             -M {params.email} \
             -m bea \
             workflow/get_baselayers/sh_scripts/download_clip_landfire.sh \
             {params.conda_env} \
             {wildcards.prod} \
             "{params.link}" \
             {params.checksum} \
             "{params.dir_name}" \
             {output.metadata_dir} \
             {input.ROI} \
             {output.done_flag}
        """
        # """
        # qsub workflow/get_baselayers/sh_scripts/download_clip_landfire.sh \
        #     "{log.joblog}" \
        #     {resources.runtime}:00:00 \
        #     {resources.mem_gb} \
        #     {params.conda_env} \
        #     {wildcards.prod} \
        #     "{params.link}" \
        #     {params.checksum} \
        #     "{params.dir_name}" \
        #     {output.metadata_dir}\
        #     {input.ROI} \
        #     {output.done_flag} > {log.joblog} 2>&1
        # """
        # """
        # python workflow/get_baselayers/download_clip_landfire.py \
        #     {wildcards.prod} \
        #     {params.link} \
        #     {params.checksum} \
        #     {params.dir_name} \
        #     {output.metadata_dir}\
        #     {input.ROI}  > {log.joblog} 2>&1
        # touch {output.done_flag}
        # """
    # script: 
    #     # first, wget landfire, MTBS, RAP data
    #     'workflow/get_baselayers/download_clip_landfire.py {input.ROI} {output.temp_dist_dir} {output.temp_topo_dir}' 
    #     'workflow/get_baselayers/download_mtbs.py' 
    #     'workflow/get_baselayers/download_rap.py' 
    #     # then, process landfire, MTBS, RAP data into convenient rxr objects with timexlatxlon dimensions
    #     'workflow/get_baselayers/make_hdist'
    #     'workflow/get_baselayers/make_topo.py'
    #     'workflow/get_baselayers/make_landfire_evt.py'
    #     'workflow/get_baselayers/make_mtbs_merged.py'
