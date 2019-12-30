#!/groups/ahrens/home/weiz/miniconda3/envs/myenv/bin/python

import os, sys
import warnings
warnings.filterwarnings('ignore')
from fish_proc.wholeBrainDask.cellProcessing_single_WS import *
import fish_proc.wholeBrainDask.cellProcessing_single_WS as fwc
from fish_proc.utils.fileio import make_tarfile
import dask.array as da
import numpy as np
import pandas as pd
import shutil

df = pd.read_csv('data_list.csv', index_col=0)
# dask_tmp = '/scratch/weiz/dask-worker-space'
dask_tmp = '/nrs/ahrens/Ziqiang/dask-worker-space'
memory_limit = 0 # unlimited
down_sample_registration = 3
baseline_percentile = 20
savetmp = '/scratch/weiz/'

# update the processing file first
for ind, row in df.iterrows():
    save_root = row['save_dir']
    if os.path.exists(f'{save_root}/cell_dff.npz'):
        df.at[ind, 'dFF'] = True
    if row['Processed']:
        continue
    if os.path.exists(f'{save_root}/cell_raw_dff_sparse.npz'):
        df.at[ind, 'Processed'] = True
df.to_csv('data_list.csv')

df = pd.read_csv('data_list.csv', index_col=0)

for ind, row in df.iterrows():
    dir_root = row['dat_dir'] # +'im/'
    save_root = row['save_dir']
    # parameters from excel file
    baseline_window = row['baseline_window']   # number of frames
    num_t_chunks = row['num_t_chunks']
    cameraNoiseMat = row['cameraNoiseMat']
    is_singlePlane = row['singlePlane']
    mask_max = row['mask']
    savetmp = save_root

    if row['Processed']:
        continue
    
    if not os.path.exists(save_root):
        os.makedirs(save_root)
    files = sorted(glob(dir_root+'/*.h5'))
    chunks = File(files[0],'r')['default'].shape
    nsplit = (chunks[1]//64, chunks[2]//64)
    if not os.path.exists(save_root):
        os.makedirs(save_root)
    print('========================')
    print('Preprocessing')
    if not os.path.exists(savetmp+'/motion_corrected_data_chunks_%03d.zarr'%(num_t_chunks-1)):
        preprocessing(dir_root, savetmp, cameraNoiseMat=cameraNoiseMat, nsplit=nsplit, \
                      num_t_chunks=num_t_chunks, dask_tmp=dask_tmp, memory_limit=memory_limit, \
                      is_singlePlane=is_singlePlane, is_bz2=False, \
                      down_sample_registration=down_sample_registration)
    print('========================')
    print('Combining motion corrected data')
    if not os.path.exists(f'{savetmp}/motion_corrected_data.zarr'):
        combine_preprocessing(dir_root, savetmp, num_t_chunks=num_t_chunks, dask_tmp=dask_tmp, memory_limit=memory_limit)
    if not os.path.exists(f'{savetmp}/detrend_data.zarr'):
        detrend_data(dir_root, savetmp, window=baseline_window, percentile=baseline_percentile, \
                     nsplit=nsplit, dask_tmp=dask_tmp, memory_limit=memory_limit)
    print('========================')
    print('Mask')
    default_mask(dir_root, savetmp, dask_tmp=dask_tmp, memory_limit=memory_limit)
    print('========================')
    print('Demix')
    dt = 3
    is_skip = True
    demix_cells(savetmp, dt, is_skip=is_skip, dask_tmp=dask_tmp, memory_limit=memory_limit)
    
    # remove some files --
    Y_d = da.from_zarr(f'{savetmp}/Y_max.zarr')
    np.save(f'{save_root}/Y_max', Y_d.compute())
    Y_d = da.from_zarr(f'{savetmp}/Y_d_max.zarr')
    np.save(f'{save_root}/Y_d_max', Y_d.compute())
    Y_d = da.from_zarr(f'{savetmp}/Y_ave.zarr')
    chunks = Y_d.chunksize[:-1]
    np.save(f'{save_root}/Y_ave', Y_d.compute())
    np.save(f'{save_root}/chunks', chunks)
    
    for nfolder in glob(savetmp+'/Y_*.zarr/'):
        shutil.rmtree(nfolder)
    shutil.rmtree(f'{savetmp}/detrend_data.zarr')
    make_tarfile(save_root+'sup_demix_rlt.tar.gz', savetmp+'sup_demix_rlt')
    
    Y_d = np.load(f'{save_root}/Y_ave.npy')
    chunks = np.load(f'{save_root}/chunks.npy')
    Y_d_max = Y_d.max(axis=0, keepdims=True)
    
    max_ = np.percentile(Y_d_max, mask_max)
    mask_ = Y_d_max>max_
    mask_ = np.repeat(mask_, Y_d.shape[0], axis=0)
    mask_ = da.from_array(mask_, chunks=(1, chunks[1], chunks[2], -1))
    
    print('========================')
    print('DF/F computation')
    compute_cell_dff_raw(savetmp, mask_, dask_tmp=dask_tmp, memory_limit=0)
    combine_dff(savetmp)
    combine_dff_sparse(savetmp)
    
    shutil.move(f'{savetmp}/cell_raw_dff_sparse.npz', f'{save_root}/cell_raw_dff_sparse.npz')
    shutil.move(f'{savetmp}/cell_raw_dff.npz', f'{save_root}/cell_raw_dff.npz')
    
    for nfolder in glob(savetmp+'*.zarr/'):
        shutil.rmtree(nfolder)
    shutil.rmtree(savetmp+'cell_raw_dff')
    shutil.rmtree(savetmp+'sup_demix_rlt')