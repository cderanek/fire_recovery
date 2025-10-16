import subprocess, sys, re

jobid = sys.argv[1]

try:
    # this should work only for active jobs (queued or running)
    output = subprocess.check_output(['qstat', '-j', jobid],
                                                stderr=subprocess.STDOUT,
                                                text=True)
    print('running')

except:
    try:
        # this should work for completed jobs (successful or failed)
        acct_output = subprocess.check_output(['qacct', '-j', jobid],
                                                stderr=subprocess.STDOUT,
                                                text=True)

        exit_status_match = re.search(r'exit_status\s+(\d+)', acct_output)

        if exit_status_match:
            exit_status = int(exit_status_match.group(1))
            if exit_status == 0:
                print('success')
            else:
                print('failed')
        else:
            print('failed')
        
    except:
        print('failed')
