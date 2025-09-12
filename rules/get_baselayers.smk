configfile: 'configs/config.yml'
sys.path.append('rules/')
from common import get_path

# Set paths based on testing mode
TESTING = config['TESTING']
if TESTING:
    ROI_PATH = config['TEST_ROI']
else:
    ROI_PATH = config['ROI']

LANDFIRE_PRODUCTS = list(config['LANDFIRE_PRODUCTS'].keys())
BASELAYER_FILES = list(config['BASELAYERS'].keys())
TOPO_LAYERS = ['Asp', 'Elev', 'Slope']
start_year, end_year = config['RECOVERY_PARAMS']['START_YEAR'], config['RECOVERY_PARAMS']['END_YEAR']
YEARS_TO_PROCESS = list(range(start_year, end_year + 1))


# Rule for landfire download
rule get_landfire:
    output: 
        ## TEMP output of downloading initial CONUS-wide files
        # LANDFIRE
        done_flag=get_path("logs/baselayers/done/landfire_{prod}_processed.done", ROI_PATH),

        ## DATA DOWNLOAD info (date downloaded, version, metadata, etc)
        metadata_dir=directory(get_path('data/baselayers/downloadlogs_metadata/{prod}', ROI_PATH))

    params:
        ROI=ROI_PATH, # Our ROI to clip to (assumed to be in CONUS)
        link=lambda wildcards: config['LANDFIRE_PRODUCTS'][wildcards.prod]['link'],
        checksum=lambda wildcards: config['LANDFIRE_PRODUCTS'][wildcards.prod]['checksum'],
        dir_name=lambda wildcards: get_path(config['LANDFIRE_PRODUCTS'][wildcards.prod]['dir_name'], ROI_PATH),
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']

    conda: 
        '../workflow/envs/get_baselayers_env.yml'

    log: 
        stdout=get_path('logs/baselayers/get_landfire_{prod}.log', ROI_PATH),
        stderr=get_path('logs/baselayers/get_landfire_{prod}.err', ROI_PATH)

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
        get_path("logs/baselayers/done/landfire_Disturbance_processed.done", ROI_PATH), # need to have successfully downloaded all the disturbance data

    output:
        merged_out_path=get_path(config['BASELAYERS']['annual_dist']['fname'], ROI_PATH),
        done_flag=get_path("logs/baselayers/done/make_hdist.done", ROI_PATH)

    params:
        annual_dist_dir=get_path(config['LANDFIRE_PRODUCTS']['Disturbance']['dir_name'], ROI_PATH),
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
        '../workflow/envs/get_baselayers_env.yml'

    log:
        stdout=get_path('logs/baselayers/make_hdist.log', ROI_PATH),
        stderr=get_path('logs/baselayers/make_hdist.err', ROI_PATH)

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
        get_path("logs/baselayers/done/download_nlcd.done", ROI_PATH)

    output: 
        # Output flag for merged agdev mask .nc
        merged_out_path=get_path(config['BASELAYERS']['agdev_mask']['fname'], ROI_PATH),
        done_flag=get_path("logs/baselayers/done/agdev_mask.done", ROI_PATH)

    params:
        nlcd_dir=get_path(config['NLCD']['dir_name'], ROI_PATH),
        vegcodes_csv=get_path(config['NLCD']['vegcodes_csv'], ROI_PATH),
        dtype=config['BASELAYERS']['agdev_mask']['dtype'],
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']

    resources:
        mem_gb=100

    conda: 
        '../workflow/envs/get_baselayers_env.yml'

    log: 
        stdout=get_path('logs/baselayers/agdev_mask.log', ROI_PATH),
        stderr=get_path('logs/baselayers/agdev_mask.err', ROI_PATH)

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
        done_flag=get_path("logs/baselayers/done/mtbs_bundles.done", ROI_PATH),
        # add another output for fireids list
        allfirestxt=get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_CONFIGS']}allfilteredfires.txt', ROI_PATH)

    params:
        ROI=ROI_PATH,
        wumi_subfires_csv=config['WUMI_PRODUCTS']['subfires_csv_f'],
        wumi_proj=config['WUMI_PRODUCTS']['projection_raster'],
        wumi_data_dir=config['WUMI_PRODUCTS']['data_dir'],
        mtbs_sevrasters_dir=config['WUMI_PRODUCTS']['mtbs_rasters_dir'],
        start_year=start_year,
        end_year=end_year,
        output_dir=get_path(config['RECOVERY_PARAMS']['RECOVERY_MAPS_DIR'], ROI_PATH),
        wumi_summary_output_dir=get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_CONFIGS']}wumi_data.csv', ROI_PATH),
        conda_env='RIO_GPD',
        threads=6,
        email=config['NOTIFY_EMAIL']

    resources:
        mem_gb=50,
        cpus=1

    conda: 
        '../workflow/envs/get_baselayers_env.yml'

    log: 
        stdout=get_path('logs/baselayers/mtbs_bundles.log', ROI_PATH),
        stderr=get_path('logs/baselayers/mtbs_bundles.err', ROI_PATH)

    shell:
        """
        workflow/get_baselayers/sh_scripts/make_mtbs_bundles.sh \
             {params.conda_env} \
             {params.ROI} \
             {params.wumi_subfires_csv} \
             {params.wumi_proj} \
             {params.wumi_data_dir} \
             {params.mtbs_sevrasters_dir} \
             {params.start_year} \
             {params.end_year} \
             {params.output_dir} \
             {params.wumi_summary_output_dir} \
             {output.allfirestxt} \
             {output.done_flag} \
             {params.threads}  > {log.stdout} 2> {log.stderr}
        """


rule merge_topo:
    input:
        expand(get_path("logs/baselayers/done/landfire_{prod}_processed.done", ROI_PATH), prod=TOPO_LAYERS)

    output:
        out_f=get_path(config['BASELAYERS']['topo']['fname'], ROI_PATH),
        done_flag=get_path("logs/baselayers/done/merge_topo.done", ROI_PATH)

    params:
        elev_dir=get_path(config['LANDFIRE_PRODUCTS']['Elev']['dir_name'], ROI_PATH),
        asp_dir=get_path(config['LANDFIRE_PRODUCTS']['Asp']['dir_name'], ROI_PATH),
        slope_dir=get_path(config['LANDFIRE_PRODUCTS']['Slope']['dir_name'], ROI_PATH),
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']

    conda: 
        '../workflow/envs/get_baselayers_env.yml'

    log: 
        stdout=get_path('logs/baselayers/merge_topo.log', ROI_PATH),
        stderr=get_path('logs/baselayers/merge_topo.err', ROI_PATH)

    shell:
        """
        workflow/get_baselayers/sh_scripts/merge_topo.sh \
        {params.conda_env} \
        {params.elev_dir} \
        {params.asp_dir} \
        {params.slope_dir} \
        {output.out_f} \
        {output.done_flag}  > {log.stdout} 2> {log.stderr}
        """
        

rule download_nlcd:
    output:
        done_flag=get_path("logs/baselayers/done/download_nlcd.done", ROI_PATH)

    params:
        download_link=config['NLCD']['annual_nlcd_link'],
        out_dir=get_path(config['NLCD']['dir_name'], ROI_PATH),
        vegcodes_csv=get_path(config['NLCD']['vegcodes_csv'], ROI_PATH),
        start_year=start_year,
        end_year=end_year,
        ROI=ROI_PATH,
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']

    conda: 
        '../workflow/envs/get_baselayers_env.yml'

    log: 
        stdout=get_path('logs/baselayers/download_nlcd.log', ROI_PATH),
        stderr=get_path('logs/baselayers/download_nlcd.err', ROI_PATH)

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
        {output.done_flag}  > {log.stdout} 2> {log.stderr}
        """


rule make_groupings:
    input:
        get_path("logs/baselayers/done/download_nlcd.done", ROI_PATH),
        get_path("logs/baselayers/done/merge_topo.done", ROI_PATH)

    output:
        out_f=get_path(config['BASELAYERS']['groupings']['fname'], ROI_PATH),
        done_flag=get_path("logs/baselayers/done/make_groupings.done", ROI_PATH)

    params:
        elev_band_m=config['RECOVERY_PARAMS']['ELEV_BANDS_METERS'],
        nlcd_dir=get_path(config['NLCD']['dir_name'], ROI_PATH),
        vegcodes_csv=get_path(config['NLCD']['vegcodes_csv'], ROI_PATH),
        merged_topo=get_path(config['BASELAYERS']['topo']['fname'], ROI_PATH),
        conda_env='RIO_GPD',
        email=config['NOTIFY_EMAIL']

    conda: 
        '../workflow/envs/get_baselayers_env.yml'

    log: 
        stdout=get_path('logs/baselayers/make_groupings.log', ROI_PATH),
        stderr=get_path('logs/baselayers/make_groupings.err', ROI_PATH)

    shell:
        """
        workflow/get_baselayers/sh_scripts/make_groupings.sh \
        {params.conda_env} \
        {params.elev_band_m} \
        {params.nlcd_dir} \
        {params.vegcodes_csv} \
        {params.merged_topo} \
        {output.out_f} \
        {output.done_flag}  > {log.stdout} 2> {log.stderr}
        """