import os
configfile: 'configs/config.yml'

# Set paths based on testing mode
TESTING = config['TESTING']
if TESTING:
    ROI_PATH = config['TEST_ROI']
    DATA_PREFIX = os.path.join('data/test_data/output', os.path.splitext(os.path.basename(ROI_PATH))[0])
else:
    ROI_PATH = config['ROI']

# Helper fn to add prefix when in testing mode
def get_path(path):
    if TESTING:
        if path.startswith("data/") or path.startswith("logs/"):
            return path.replace("data/", f"{DATA_PREFIX}/", 1)
    return path

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
        [get_path(config['BASELAYERS'][prod]['fname']) for prod in BASELAYER_FILES],
        get_path("data/baselayers/mtbs_bundles.done")

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    log: 
        stdout=get_path('logs/get_baselayers.log'),
        stderr=get_path('logs/get_baselayers.err')

    output:
        done_flag=get_path('data/baselayers/all_baselayers_merged.done')

    shell: "touch {output.done_flag}  > {log.stdout} 2> {log.stderr}"


# Rule for landfire download
rule get_landfire:
    output: 
        ## TEMP output of downloading initial CONUS-wide files
        # LANDFIRE
        done_flag=get_path("data/baselayers/landfire_{prod}_processed.done"),

        ## DATA DOWNLOAD info (date downloaded, version, metadata, etc)
        metadata_dir=directory(get_path('data/baselayers/downloadlogs_metadata/{prod}'))

    params:
        ROI=ROI_PATH, # Our ROI to clip to (assumed to be in CONUS)
        link=lambda wildcards: config['LANDFIRE_PRODUCTS'][wildcards.prod]['link'],
        checksum=lambda wildcards: config['LANDFIRE_PRODUCTS'][wildcards.prod]['checksum'],
        dir_name=lambda wildcards: get_path(config['LANDFIRE_PRODUCTS'][wildcards.prod]['dir_name']),
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    log: 
        stdout=get_path('logs/get_landfire_{prod}.log'),
        stderr=get_path('logs/get_landfire_{prod}.err')

    shell:
        """
        workflow/get_baselayers/sh_scripts/download_clip_landfire.sh \
             {params.conda_env} \
             {wildcards.prod} \
             {params.link} \
             {params.checksum} \
             {params.dir_name} \
             {output.metadata_dir} \
             {params.ROI} \
             {output.done_flag}  > {log.stdout} 2> {log.stderr}
        """


rule make_hdist:
    input:
        get_path("data/baselayers/landfire_Disturbance_processed.done"), # need to have successfully downloaded all the disturbance data

    output: 
        # Output flag for merged agdev mask .nc
        merged_out_path=get_path(config['BASELAYERS']['annual_dist']['fname']),
        done_flag=get_path("data/baselayers/make_hdist.done")

    params:
        annual_dist_dir=get_path(config['LANDFIRE_PRODUCTS']['Disturbance']['dir_name']),
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
        stdout=get_path('logs/make_hdist.log'),
        stderr=get_path('logs/make_hdist.err')

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
        get_path("data/baselayers/landfire_Disturbance_processed.done"), # need to have successfully downloaded all the disturbance data

    output: 
        # Output flag for merged agdev mask .nc
        merged_out_path=get_path(config['BASELAYERS']['agdev_mask']['fname']),
        done_flag=get_path("data/baselayers/agdev_mask.done")

    params:
        nlcd_dir=get_path(config['NLCD']['dir_name']),
        vegcodes_csv=get_path(config['NLCD']['vegcodes_csv']),
        dtype=config['BASELAYERS']['agdev_mask']['dtype'],
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']

    resources:
        mem_gb=100

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    log: 
        stdout=get_path('logs/agdev_mask.log'),
        stderr=get_path('logs/agdev_mask.err')

    shell:
        """
        workflow/get_baselayers/sh_scripts/make_agdev_mask.sh \
             {params.conda_env} \
             {params.nlcd_dir} \
             {params.vegcodes_csv} \
             {output.merged_out_path} \
             {params.dtype} \
             {output.done_flag}  > {log.stdout} 2> {log.stderr}
        """


rule make_mtbs_bundles:
    output:
        done_flag=get_path("data/baselayers/mtbs_bundles.done")

    params:
        wumi_subfires_csv=config['WUMI_PRODUCTS']['subfires_csv_f'],
        wumi_proj=config['WUMI_PRODUCTS']['projection_raster'],
        wumi_data_dir=get_path(config['WUMI_PRODUCTS']['data_dir']),
        mtbs_sevrasters_dir=config['WUMI_PRODUCTS']['mtbs_rasters_dir'],
        start_year=config['START_YEAR'],
        end_year=config['END_YEAR'],
        output_dir=get_path(config['RECOVERY_MAPS_DIR']),
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']

    resources:
        mem_gb=50,
        cpus=1

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    log: 
        stdout=get_path('logs/mtbs_bundles.log'),
        stderr=get_path('logs/mtbs_bundles.err')

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
        expand(get_path("data/baselayers/landfire_{prod}_processed.done"), prod=TOPO_LAYERS)

    output:
        out_f=get_path(config['BASELAYERS']['topo']['fname']),
        done_flag=get_path("data/baselayers/merge_topo.done")

    params:
        elev_dir=get_path(config['LANDFIRE_PRODUCTS']['Elev']['dir_name']),
        asp_dir=get_path(config['LANDFIRE_PRODUCTS']['Asp']['dir_name']),
        slope_dir=get_path(config['LANDFIRE_PRODUCTS']['Slope']['dir_name']),
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    log: 
        stdout=get_path('logs/merge_topo.log'),
        stderr=get_path('logs/merge_topo.err')

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
        done_flag=get_path("data/baselayers/download_nlcd.done")

    params:
        download_link=config['NLCD']['annual_nlcd_link'],
        out_dir=get_path(config['NLCD']['dir_name']),
        vegcodes_csv=get_path(config['NLCD']['vegcodes_csv']),
        start_year=config['START_YEAR'],
        end_year=config['END_YEAR'],
        ROI=ROI_PATH,
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    log: 
        stdout=get_path('logs/download_nlcd.log'),
        stderr=get_path('logs/download_nlcd.err')

    shell:
        """
        workflow/get_baselayers/sh_scripts/download_clip_nlcd.sh \
        {params.conda_env} \
        {params.download_link} \
        {params.out_dir} \
        {params.vegcodes_csv} \
        {params.start_year} \
        {params.end_year} \
        {params.ROI} \
        {output.done_flag}
        """


rule make_groupings:
    input:
        get_path("data/baselayers/download_nlcd.done"),
        get_path("data/baselayers/merge_topo.done")

    output:
        out_f=get_path(config['BASELAYERS']['groupings']['fname'])

    params:
        elev_band_m=config['ELEV_BANDS_METERS'],
        nlcd_dir=get_path(config['NLCD']['dir_name']),
        vegcodes_csv=get_path(config['NLCD']['vegcodes_csv']),
        merged_topo=get_path(config['BASELAYERS']['topo']['fname']),
        out_f=get_path(config['BASELAYERS']['groupings']['fname']),
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    log: 
        stdout=get_path('logs/make_groupings.log'),
        stderr=get_path('logs/make_groupings.err')

    shell:
        """
        workflow/get_baselayers/sh_scripts/make_groupings.sh \
        {params.conda_env} \
        {params.elev_band_m} \
        {params.nlcd_dir} \
        {params.vegcodes_csv} \
        {params.merged_topo} \
        {params.out_f} \
        {output.done_flag}
        """