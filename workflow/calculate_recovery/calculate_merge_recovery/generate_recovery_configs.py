import json, sys

'''
needs main config for recovery params, landsat download params, wumi info
## Generate main recovery config -- not per-fire specific
Uses main config, drops all RECOVERY_PARAMS to json
Also adds in path to baselayers
Also adds in Landsat download params

## Generate per-fire recovery configs
Dict with keys = fireid
{
    fireid:
        fire_metadata:

        file_paths: 
}
'''

if __name__ == '__main__':
    print(f'Running generate_recovery_configs.py with arguments {'\n'.join(sys.argv)}\n')
    config_json = json.loads(sys.argv[1])
    fireslist_txt = sys.argv[2]
    done_flag = sys.argv[3]