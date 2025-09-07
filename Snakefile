configfile: "configs/config.yml"
LANDFIRE_PRODUCTS = list(config['LANDFIRE_PRODUCTS'].keys())
BASELAYER_FILES = list(config['BASELAYERS'].keys())
TOPO_LAYERS = ['Asp', 'Elev', 'Slope']
start_year, end_year = config['START_YEAR'], config['END_YEAR']
YEARS_TO_PROCESS = list(range(start_year, end_year + 1))

### Define targets for the full workflow (coming soon) ###
# rule all:
#     input:
#         # tbd


### Download baselayers for full ROI ###
# Trigger product downloads for all baselayers

## TODO: Once all final baselayers are confirmed to be completed, set intermediated output files as temp() in the relevant rules
## TODO: Figure out how to combine workflows -- main workflow should call build baselayers workflow, build recovery maps workflow, sampling workflow, and analysis/vis workflow

rule get_baselayers:
    input:
        # # LANDFIRE inputs
        # expand("data/baselayers/landfire_{prod}_processed.done", prod=LANDFIRE_PRODUCTS),
        # # RAP input, annual
        # # expand("data/baselayers/rap_{year}_processed.done", year=YEARS_TO_PROCESS),
        # # Baselayers outputs
        # "data/baselayers/make_hdist.done",
        # "data/baselayers/agdev_mask.done",
        # 
        # # Eventually replace individual baselayers AND the origianl landfire/rap files with:
        [config['BASELAYERS'][prod]['fname'] for prod in BASELAYER_FILES],
        "data/baselayers/mtbs_bundles.done"

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    log: 
        stdout='logs/get_baselayers.log',
        stderr='logs/get_baselayers.err'

    output:
        done_flag="data/baselayers/all_baselayers_merged.done"

    shell: "touch {output.done_flag}  > {log.stdout} 2> {log.stderr}"


# Rule for landfire download
rule get_landfire:
    output: 
        ## TEMP output of downloading initial CONUS-wide files
        # LANDFIRE
        done_flag="data/baselayers/landfire_{prod}_processed.done",

        ## DATA DOWNLOAD info (date downloaded, version, metadata, etc)
        metadata_dir=directory('data/baselayers/downloadlogs_metadata/{prod}')

    params:
        ROI=config['ROI'], # Our ROI to clip to (assumed to be in CONUS)
        link=lambda wildcards: config['LANDFIRE_PRODUCTS'][wildcards.prod]['link'],
        checksum=lambda wildcards: config['LANDFIRE_PRODUCTS'][wildcards.prod]['checksum'],
        dir_name=lambda wildcards: config['LANDFIRE_PRODUCTS'][wildcards.prod]['dir_name'],
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    log: 
        stdout='logs/get_landfire_{prod}.log',
        stderr='logs/get_landfire_{prod}.err'

    shell:
        """
        workflow/get_baselayers/sh_scripts/download_clip_landfire.sh \
             {params.conda_env} \
             {wildcards.prod} \
             "{params.link}" \
             {params.checksum} \
             "{params.dir_name}" \
             {output.metadata_dir} \
             {params.ROI} \
             {output.done_flag}  > {log.stdout} 2> {log.stderr}
        """


rule get_rap:
    output: 
        ## TEMP output of downloading initial unmerged files
        # RAP
        done_flag="data/baselayers/rap_{year}_processed.done"

        # Small output clipped from permanent output (these will be used for testing)

    resources:
        mem_gb=60
    
    params:
        ROI=config['ROI'], # Our ROI to clip to (assumed to be in CONUS)
        link=config['RAP_PRODUCTS']['veg_cover_link_prefix'],
        checksum=config['RAP_PRODUCTS']['checksum_ref'],
        dir_name=config['RAP_PRODUCTS']['dir_name'],
        year=lambda wildcards: wildcards.year,
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']
    
    #group: "rap_download_group" # run all RAP downloads as a single qsub job, to avoid making a bunch of short jobs in the queue

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    log: 
        stdout='logs/rap/get_rap_{year}.log',
        stderr='logs/rap/get_rap_{year}.err'

    shell:
        """
        workflow/get_baselayers/sh_scripts/download_clip_rap.sh \
             {params.conda_env} \
             {params.link} \
             {params.checksum} \
             {params.year} \
             {params.ROI} \
             {params.dir_name} \
             {output.done_flag} > {log.stdout} 2> {log.stderr}
        """


rule make_hdist:
    input:
        "data/baselayers/landfire_Disturbance_processed.done", # need to have successfully downloaded all the disturbance data

    output: 
        # Output flag for merged agdev mask .nc
        merged_out_path=config['BASELAYERS']['annual_dist']['fname'],
        done_flag="data/baselayers/make_hdist.done"

        # Small output clipped from permanent output (these will be used for testing)

    params:
        annual_dist_dir=config['LANDFIRE_PRODUCTS']['Disturbance']['dir_name'],
        dtype=config['BASELAYERS']['annual_dist']['dtype'],
        nodataval=config['BASELAYERS']['annual_dist']['nodataval'],
        xdim=config['BASELAYERS']['annual_dist']['dims']['xdim'],
        ydim=config['BASELAYERS']['annual_dist']['dims']['ydim'],
        timedim=config['BASELAYERS']['annual_dist']['dims']['timedim'],
        start_year=config['LANDFIRE_PRODUCTS']['Disturbance']['start_year'],
        end_year=config['LANDFIRE_PRODUCTS']['Disturbance']['end_year'],
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']
    
    resources:
        mem_gb=100

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    log:
        stdout='logs/make_hdist.log',
        stderr='logs/make_hdist.err'

    shell:
        """
        workflow/get_baselayers/sh_scripts/make_hdist.sh \
             {params.conda_env} \
             {params.annual_dist_dir} \
             {output.merged_out_path} \
             {params.nodataval} \
             {params.dtype} \
             {params.xdim} \
             {params.ydim} \
             {params.timedim} \
             {params.start_year} \
             {params.end_year} \
             {output.done_flag} > {log.stdout} 2> {log.stderr}
        """
        
        
rule make_agdevmask:
    input:
        "data/baselayers/landfire_Disturbance_processed.done", # need to have successfully downloaded all the disturbance data
        "data/baselayers/landfire_EVT_2001_processed.done",
        "data/baselayers/landfire_EVT_2016_processed.done",
        "data/baselayers/landfire_EVT_2020_processed.done",
        "data/baselayers/landfire_EVT_2022_processed.done",
        "data/baselayers/landfire_EVT_2023_processed.done",
        "data/baselayers/landfire_EVT_2024_processed.done"

    output: 
        # Output flag for merged agdev mask .nc
        merged_out_path=config['BASELAYERS']['agdev_mask']['fname'],
        done_flag="data/baselayers/agdev_mask.done"

    params:
        evt_dir=config['LANDFIRE_PRODUCTS']['EVT_2001']['dir_name'],
        dtype=config['BASELAYERS']['agdev_mask']['dtype'],
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']

    resources:
        mem_gb=100

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    log: 
        stdout='logs/agdev_mask.log',
        stderr='logs/agdev_mask.err'

    shell:
        """
        workflow/get_baselayers/sh_scripts/make_agdev_mask.sh \
             {params.conda_env} \
             {params.evt_dir} \
             {output.merged_out_path} \
             {params.dtype} \
             {output.done_flag}  > {log.stdout} 2> {log.stderr}
        """


rule make_mtbs_bundles:
    output:
        done_flag="data/baselayers/mtbs_bundles.done"

    params:
        wumi_subfires_csv=config['WUMI_PRODUCTS']['subfires_csv_f'],
        wumi_proj=config['WUMI_PRODUCTS']['projection_raster'],
        wumi_data_dir=config['WUMI_PRODUCTS']['data_dir'],
        mtbs_sevrasters_dir=config['WUMI_PRODUCTS']['mtbs_rasters_dir'],
        start_year=config['START_YEAR'],
        end_year=config['END_YEAR'],
        output_dir=config['RECOVERY_MAPS_DIR'],
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']

    resources:
        mem_gb=50,
        cpus=1

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    log: 
        stdout='logs/mtbs_bundles.log',
        stderr='logs/mtbs_bundles.err'

    shell:
        """
        workflow/get_baselayers/sh_scripts/make_mtbs_bundles.sh \
             {params.conda_env} \
             {params.wumi_subfires_csv} \
             {params.wumi_proj} \
             {params.wumi_data_dir} \
             {params.mtbs_sevrasters_dir} \
             {params.start_year} \
             {params.end_year} \
             {params.output_dir} \
             {output.done_flag} \
             {resources.cpus}  > {log.stdout} 2> {log.stderr}
        """


rule merge_topo:
    input:
        expand("data/baselayers/landfire_{prod}_processed.done", prod=TOPO_LAYERS)

    output:
        out_f=config['BASELAYERS']['topo']['fname'],
        done_flag="data/baselayers/merge_topo.done"

    params:
        elev_dir=config['LANDFIRE_PRODUCTS']['Elev']['dir_name'],
        asp_dir=config['LANDFIRE_PRODUCTS']['Asp']['dir_name'],
        slope_dir=config['LANDFIRE_PRODUCTS']['Slope']['dir_name'],
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    log: 
        stdout='logs/merge_topo.log',
        stderr='logs/merge_topo.err'

    shell:
        """
        workflow/get_baselayers/sh_scripts/merge_topo.sh \
        {params.conda_env} \
        {params.elev_dir} \
        {params.asp_dir} \
        {params.slope_dir} \
        {output.out_f} \
        {output.done_flag}
        """
        

rule download_nlcd:
    output:
        done_flag="data/baselayers/download_nlcd.done"

    params:
        download_link=config['NLCD']['annual_lc_link'],
        out_dir=config['NLCD']['dir_name'],
        metadata_dir=config['NLCD']['metadata_dir_name'],
        start_year=config['START_YEAR'],
        end_year=config['END_YEAR'],
        ROI=config['ROI'],
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    log: 
        stdout='logs/download_nlcd.log',
        stderr='logs/download_nlcd.err'

    shell:
        """
        workflow/get_baselayers/sh_scripts/download_clip_nlcd.sh \
        {params.download_link} \
        {params.out_dir} \
        {params.metadata_dir} \
        {params.start_year} \
        {params.end_year} \
        {params.ROI} \
        {output.done_flag}
        """


rule make_groupings:
    input:
        "data/baselayers/download_nlcd.done",
        "data/baselayers/merge_topo.done"

    output:
        out_f=config['BASELAYERS']['groupings']['fname']

    params:
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']

