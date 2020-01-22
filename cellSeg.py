#!/groups/ahrens/home/weiz/miniconda3/envs/myenv/bin/python

import os, sys
import warnings
warnings.filterwarnings('ignore')
from fish_proc.wholeBrainDask.cellProcessing_single_WS import *
import fish_proc.wholeBrainDask.cellProcessing_single_WS as fwc
from fish_proc.utils.fileio import make_tarfile, chmod
import dask.array as da
import numpy as np
import pandas as pd
import shutil
from cellSegProc import cellSegProc

df = pd.read_csv('data_list.csv', index_col=0)
df_ = pd.read_csv('finshed_data_list.csv', index_col=0)
dask_tmp = '/scratch/weiz/dask-worker-space'
memory_limit = 0 # unlimited
down_sample_registration = 3
baseline_percentile = 20
savetmp = '/scratch/weiz/'

if os.path.exists(savetmp+'processing.tmp'):
    f = open(savetmp+'processing.tmp', "r")
    if f.mode == 'r':
        _ =f.read()
        print(_)
    f.close()
    sys.exit()

# get new processing list
os.system("git pull");

# update the processing file first
for ind, row in df.iterrows():
    save_root = row['save_dir']
    if os.path.exists(f'{save_root}/cell_dff.npz'):
        df.at[ind, 'dFF'] = True
    if row['Processed']:
        continue
    if os.path.exists(f'{save_root}/cell_raw_dff_sparse.npz'):
        df.at[ind, 'Processed'] = True
df_.append(df[df['Processed']==True], ignore_index=True).to_csv('finshed_data_list.csv')
df[df['Processed']==False].to_csv('data_list.csv')
os.system('git add data_list.csv finshed_data_list.csv')
os.system("git commit -m 'update processing table'");
os.system("git push");


# Start processing
df = pd.read_csv('data_list.csv', index_col=0)
for ind, row in df.iterrows():
    if row['Processed']:
        continue
    
    cellSegProc(row, savetmp=savetmp, \
                dask_tmp=dask_tmp, \
                memory_limit=memory_limit, \
                baseline_percentile=baseline_percentile, \
                down_sample_registration=down_sample_registration)
    
    for nfolder in glob(savetmp+'/Y_*.zarr/'):
        shutil.rmtree(nfolder)
    shutil.rmtree(f'{savetmp}/detrend_data.zarr')
    make_tarfile(save_root+'sup_demix_rlt.tar.gz', savetmp+'sup_demix_rlt')
        
    shutil.move(f'{savetmp}/motion_fix_.h5', f'{save_root}/motion_fix_.h5')
    shutil.move(f'{savetmp}/trans_affs.npy', f'{save_root}/trans_affs.npy')
    shutil.move(f'{savetmp}/cell_raw_dff_sparse.npz', f'{save_root}/cell_raw_dff_sparse.npz')
    shutil.move(f'{savetmp}/cell_raw_dff.npz', f'{save_root}/cell_raw_dff.npz')
    
    for nfolder in glob(savetmp+'*.zarr/'):
        shutil.rmtree(nfolder)
    shutil.rmtree(savetmp+'cell_raw_dff')
    shutil.rmtree(savetmp+'sup_demix_rlt')
    
    f = open("curr.out", "w")
    f.write(f'Data at index {ind} is done processing at {save_root}')
    f.close()
    
    # change mode in case if the file is not accessible
    chmod(save_root, mode='0775')
    # remove holding file
    os.remove(savetmp+'processing.tmp')
    
    os.system("git pull");
    os.system('git add curr.out')
    os.system("git commit -m 'update processing progress'");
    os.system("git push");