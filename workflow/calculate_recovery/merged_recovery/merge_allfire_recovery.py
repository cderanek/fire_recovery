import os, re, shutil

# TODO: need to add layer for new recovery metrics, too

def extract_date(dir_name):
    # use last 8 digits of recovery_dir/fire_dir to get fire dates/incid IDs
    dir_basename = os.path.basename(dir_name.strip('/')).split('_')[1]
    match = re.search(r'(\d{8})$', dir_basename)
    if match: return int(match.group(1))
    else: return None
    

def sort_dirs_by_date(dir_list: List[str]) -> List[str]:
    # return directories sorted by date
    return sorted(dir_list, key=extract_date)


def get_ordered_fireUIDs(recovery_dir, summary_csv_path):
    ''' 
    Add unique IDs (ranging from 1-N) to processing progress summary CSV
    '''

    # check if UIDs were already made
    summary_csv = pd.read_csv(summary_csv_path)
    
    # if already made
    if 'uid' in summary_csv.columns:
        uid, fire_id = pd.read_csv(temp_csv)[['uid','fireid']]
        uid_fireID_list = list(zip(uid, fire_id))

    # otherwise, make ordered fireids csv
    else:
        # Order fires by date --> assign UID to each fire
        if recovery_dir[-1] != '/': recovery_dir += '/'
        dir_list = glob.glob(f'{recovery_dir}*')
        sorted_fires = sort_dirs_by_date(dir_list)
        fireID_list = ['_'.join(os.path.basename(f).split('_')[1:]) for f in sorted_fires]
        uid_fireID_list = list(enumerate(fireID_list, start=0)) # create unique integer IDs (1-N) for each fireid
        uid_fireID_df = pd.DataFrame(
            uid_fireID_list,
            columns=['uid', 'fireid']
            )
        
        # create backup of original summary csv
        shutil.copy(summary_csv_path, summary_csv_path.replace('.csv', '_backup.csv'))

        # Add UID column to existing summary csv and then save output
        pd.merge([summary_csv, uid_fireID_df], on='fireid').to_csv(summary_csv_path)
    
    return uid_fireID_list


