configfile: "configs/config.yml"
LANDFIRE_PRODUCTS = list(config['LANDFIRE_PRODUCTS'].keys())
BASELAYER_FILES = list(config['BASELAYERS'].keys())
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
        # LANDFIRE inputs
        expand("data/baselayers/landfire_{prod}_processed.done", prod=LANDFIRE_PRODUCTS),
        # RAP input, annual
        expand("data/baselayers/rap_{year}_processed.done", year=YEARS_TO_PROCESS),
        # Baselayers outputs
        "data/baselayers/make_hdist.done",
        "data/baselayers/agdev_mask.done",
        "data/baselayers/mtbs_bundles.done"
        # Eventually replace individual baselayers AND the origianl landfire/rap files with:
        # expand(config['BASELAYERS'][{prod}]['fname'], prod=BASELAYER_FILES),
        # still need to add test cases for small ROIs + add path to small ROIs in config

    log:
        "logs/get_all_baselayers.log"

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    output:
        done_flag="data/baselayers/all_baselayers_merged.done"

    shell:
        """
        touch {output.done_flag}
        """


# Rule for landfire download
rule get_landfire:
    output: 
        ## TEMP output of downloading initial CONUS-wide files
        # LANDFIRE
        done_flag="data/baselayers/landfire_{prod}_processed.done",

        ## DATA DOWNLOAD info (date downloaded, version, metadata, etc)
        metadata_dir=directory('data/baselayers/downloadlogs_metadata/{prod}')

    log: "logs/get_landfire_{prod}.log"

    params:
        ROI=config['ROI'], # Our ROI to clip to (assumed to be in CONUS)
        link=lambda wildcards: config['LANDFIRE_PRODUCTS'][wildcards.prod]['link'],
        checksum=lambda wildcards: config['LANDFIRE_PRODUCTS'][wildcards.prod]['checksum'],
        dir_name=lambda wildcards: config['LANDFIRE_PRODUCTS'][wildcards.prod]['dir_name'],
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']

    conda: 
        'workflow/envs/get_baselayers_env.yml'

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
             {output.done_flag}
        """


# Rule for RAP download
rule get_rap:
    output: 
        ## TEMP output of downloading initial unmerged files
        # RAP
        done_flag="data/baselayers/rap_{year}_processed.done"

        # Small output clipped from permanent output (these will be used for testing)

    log: "logs/rap/get_rap_{year}.log"

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
    
    group: "rap_download_group" # run all RAP downloads as a single qsub job, to avoid making a bunch of short jobs in the queue

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    shell:
        """
        workflow/get_baselayers/sh_scripts/download_clip_rap.sh \
             {params.conda_env} \
             {params.link} \
             {params.checksum} \
             {params.year} \
             {params.ROI} \
             {params.dir_name} \
             {output.done_flag}
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

    log: "logs/make_hdist.log"
    
    resources:
        mem_gb=100

    conda: 
        'workflow/envs/get_baselayers_env.yml'

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
             {output.done_flag}
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

    log: "logs/agdev_mask.log"

    resources:
        mem_gb=100

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    shell:
        """
        workflow/get_baselayers/sh_scripts/make_agdev_mask.sh \
             {params.conda_env} \
             {params.evt_dir} \
             {output.merged_out_path} \
             {params.dtype} \
             {output.done_flag}
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

    log: "logs/mtbs_bundles.log"

    resources:
        mem_gb=50,
        cpus=6

    conda: 
        'workflow/envs/get_baselayers_env.yml'

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
             {resources.cpus}
        """

