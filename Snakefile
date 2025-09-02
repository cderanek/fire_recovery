configfile: "configs/config.yml"
LANDFIRE_PRODUCTS = list(config['LANDFIRE_PRODUCTS'].keys())
start_year, end_year = config['START_YEAR'], config['END_YEAR']
YEARS_TO_PROCESS = list(range(start_year, end_year + 1))

### Define targets for the full workflow (coming soon) ###
# rule all:
#     input:
#         # tbd


### Download baselayers for full ROI ###
# Trigger product downloads for all baselayers


rule get_baselayers:
    input:
        # LANDFIRE inputs
        expand("data/baselayers/landfire_{prod}_processed.done", prod=LANDFIRE_PRODUCTS),
        # RAP input, annual
        expand("data/baselayers/rap_{year}_processed.done", year=YEARS_TO_PROCESS),
        # Baselayers outputs
        # config['BASELAYERS']['groupings']['fname'],
        # config['BASELAYERS']['topo']['fname'],
        # config['BASELAYERS']['annual_dist']['fname'],
        # config['BASELAYERS']['mtbs_sev']['fname'],
        # config['BASELAYERS']['mtbs_poly']['fname']
        # still need to add test cases for small ROIs + add path to small ROIs in config


# Rule for landfire download
rule get_landfire:
    input:
        ROI=config['ROI'] # Our ROI to clip to (assumed to be in CONUS)

    output: 
        ## TEMP output of downloading initial CONUS-wide files
        # LANDFIRE
        done_flag="data/baselayers/landfire_{prod}_processed.done",

        ## DATA DOWNLOAD info (date downloaded, version, metadata, etc)
        metadata_dir='data/baselayers/downloadlogs_metadata/{prod}'

        # Small output clipped from permanent output (these will be used for testing)

    log:
        "logs/get_landfire_{prod}.log"

    params:
        link=lambda wildcards: config['LANDFIRE_PRODUCTS'][wildcards.prod]['link'],
        checksum=lambda wildcards: config['LANDFIRE_PRODUCTS'][wildcards.prod]['checksum'],
        dir_name=lambda wildcards: config['LANDFIRE_PRODUCTS'][wildcards.prod]['dir_name'],
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']
    
    resources:
        mem_gb=20,
        runtime=12,
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

# Rule for RAP download
rule get_rap:
    input:
        ROI=config['ROI'] # Our ROI to clip to (assumed to be in CONUS)

    output: 
        ## TEMP output of downloading initial unmerged files
        # RAP
        done_flag="data/baselayers/rap_{year}_processed.done"

        # Small output clipped from permanent output (these will be used for testing)

    log:
        "logs/get_rap_{year}.log"

    params:
        link=config['RAP_PRODUCTS']['veg_cover_link_prefix'],
        checksum=config['RAP_PRODUCTS']['checksum_ref'],
        dir_name=config['RAP_PRODUCTS']['dir_name'],
        year=lambda wildcards: wildcards.year,
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']
    
    resources:
        mem_gb=60, # original download is float32 dtype -- request more memory to open/convert to int8
        runtime=12,
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
             workflow/get_baselayers/sh_scripts/download_clip_rap.sh \
             {params.conda_env} \
             {params.link} \
             {params.checksum} \
             {params.year} \
             {input.ROI} \
             {params.dir_name} \
             {output.done_flag}
        """