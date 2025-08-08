### Define targets for the full workflow ###
rule all:
    pass

### Download baselayers for full ROI ###
rule get_baselayers:
    input:
        ROI=config['ROI'] # Our ROI to clip to (assumed to be in CONUS)

    output: 
        ## TEMP output of downloading initial CONUS-wide files
        # LANDFIRE
        temp_dist_dir: temp(config['Disturbance']['dir_name'])
        temp_topo_dir: temp(config['Topo']['dir_name'])

        ## DATA PROVENANCE info (date downloaded, version, metadata, etc)
        # TODO: will be stored in data/baselayers/downloadlogs_metadata/

        ## LOGS
        # TODO: will be stored in logs/get_baselayers.log


        ## PERMANENT output
        # LANDFIRE
        groupings=config['BASELAYERS']['groupings']['fname']
        topo=config['BASELAYERS']['topo']['fname']
        annual_dist=config['BASELAYERS']['annual_dist']['fname']

        # MTBS
        mtbs_sev=config['BASELAYERS']['mtbs_sev']['fname']
        mtbs_poly=config['BASELAYERS']['mtbs_poly']['fname']

        # RAP

        # Small output clipped from permanent output (these will be used for testing)

    resources:
        mem_gb=50,
        runtime=24,
        cpus=1

    conda: 
        "workflow/envs/get_baselayers_env.yml"  # rule-specific environment

    # shell:
    # script: 
    #     # first, wget landfire, MTBS, RAP data
    #     "workflow/get_baselayers/download_clip_landfire.py {input.ROI} {output.temp_dist_dir} {output.temp_topo_dir}" 
    #     "workflow/get_baselayers/download_mtbs.py" 
    #     "workflow/get_baselayers/download_rap.py" 
    #     # then, process landfire, MTBS, RAP data into convenient rxr objects with timexlatxlon dimensions
    #     "workflow/get_baselayers/make_hdist"
    #     "workflow/get_baselayers/make_topo.py"
    #     "workflow/get_baselayers/make_landfire_evt.py"
    #     "workflow/get_baselayers/make_mtbs_merged.py"
