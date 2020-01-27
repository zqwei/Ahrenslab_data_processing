#!/groups/ahrens/home/weiz/miniconda3/envs/myenv/bin/python

import os, sys
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import shutil
from cellSegFuc import cellSegProc
from fish_proc.utils.fileio import make_tarfile, chmod
from glob import glob
from time import sleep
dask_tmp = '/scratch/weiz/dask-worker-space'
memory_limit = 0 # unlimited
down_sample_registration = 3
baseline_percentile = 20
savetmp = '/scratch/weiz/'



def update_processing_table():
    # get new processing list
    os.system("git pull");
    df = pd.read_csv('data_list.csv', index_col=0)
    df_ = pd.read_csv('finshed_data_list.csv', index_col=0)
    # update the processing file first
    for ind, row in df.iterrows():
        save_root = row['save_dir']
        if os.path.exists(f'{save_root}/cell_dff.npz'):
            df.at[ind, 'dFF'] = True
        if row['Processed']:
            continue
        if os.path.exists(f'{save_root}/cell_raw_dff_sparse.npz'):
            df.at[ind, 'Processed'] = True
    # add finished processing to finish table and remove it from processing table
    df_.append(df[df['Processed']==True], ignore_index=True).to_csv('finshed_data_list.csv')
    df[df['Processed']==False].reset_index(drop=True).to_csv('data_list.csv')
    os.system('git add data_list.csv finshed_data_list.csv')
    os.system("git commit -m 'update processing table'");
    os.system("git push");


def process_zero_row():
    df = pd.read_csv('data_list.csv', index_col=0)
    try:
        row = df.iloc[0]
    except:
        print('No new data currently -- keep looping in background')
        sleep(3600) # sleep 1 hours before another around of processing
        return None
    if row['Processed']:
        return None
    
    save_root = row['save_dir']

    # add holding file
    f = open(savetmp+'processing.tmp', "w")
    f.write(f'Data at index 0 is processing at {save_root}')
    f.close()
    
    cellSegProc(row, savetmp=savetmp, \
                dask_tmp=dask_tmp, \
                memory_limit=memory_limit, \
                baseline_percentile=baseline_percentile, \
                down_sample_registration=down_sample_registration)
    
    # folder operations
    for nfolder in glob(savetmp+'/*.zarr/'):
        shutil.rmtree(nfolder)
    make_tarfile(save_root+'sup_demix_rlt.tar.gz', savetmp+'sup_demix_rlt')        
    shutil.move(f'{savetmp}/motion_fix_.h5', f'{save_root}/motion_fix_.h5')
    shutil.move(f'{savetmp}/trans_affs.npy', f'{save_root}/trans_affs.npy')
    shutil.move(f'{savetmp}/cell_raw_dff_sparse.npz', f'{save_root}/cell_raw_dff_sparse.npz')
    shutil.move(f'{savetmp}/cell_raw_dff.npz', f'{save_root}/cell_raw_dff.npz')
    shutil.rmtree(savetmp+'cell_raw_dff')
    shutil.rmtree(savetmp+'sup_demix_rlt')
    # change mode in case if the file is not accessible
    chmod(save_root, mode='0775')
    # remove holding file
    os.remove(savetmp+'processing.tmp')
    
#     f = open("curr.out", "w")
#     f.write(f'Data at index 0 is done processing at {save_root}')
#     f.close()    
#     os.system("git pull");
#     os.system('git add curr.out')
#     os.system("git commit -m 'update processing progress'");
#     os.system("git push");
    return None
    
    
if __name__ == "__main__":
    while True:
        # exit if some file is processing
        if os.path.exists(savetmp+'processing.tmp'):
            f = open(savetmp+'processing.tmp', "r")
            if f.mode == 'r':
                _ =f.read()
                print(_)
            f.close()
            sys.exit()
        # update processing file
        update_processing_table()
        
        # processing the first file
        process_zero_row()