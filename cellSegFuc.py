import os
import warnings
warnings.filterwarnings('ignore')
from fish_proc.wholeBrainDask.cellProcessing_single_WS import *
import fish_proc.wholeBrainDask.cellProcessing_single_WS as fwc
import dask.array as da
import numpy as np


dask_tmp = '/scratch/weiz/dask-worker-space'
memory_limit = 0 # unlimited
baseline_percentile = 20
savetmp = '/scratch/weiz/'
is_skip = True
down_sample_registration = 3
dt = 3


def cellSegProc(row, savetmp=savetmp, \
                dask_tmp=dask_tmp, \
                memory_limit=memory_limit, \
                baseline_percentile=baseline_percentile, \
                down_sample_registration=down_sample_registration, dt=dt):
    
    dir_root = row['dat_dir']
    save_root = row['save_dir']
    print(save_root)
    # parameters from excel file
    baseline_window = row['baseline_window']   # number of frames
    num_t_chunks = row['num_t_chunks']
    cameraNoiseMat = row['cameraNoiseMat']
    is_singlePlane = row['singlePlane']
    mask_max = row['mask']
    
#     if is_singlePlane:
#         down_sample_registration = 50
#         dt = 10
#     else:
#         down_sample_registration = 3
#         dt = 3
    
    if not os.path.exists(save_root):
        os.makedirs(save_root)
    
    if not os.path.exists(f'{savetmp}/motion_corrected_data.zarr'):
        files = sorted(glob(dir_root+'/*.h5'))
        chunks = File(files[0],'r')['default'].shape
        # nsplit = (chunks[1]//64, chunks[2]//64)
        nsplit = (chunks[1]//16, chunks[2]//16)
    if not os.path.exists(save_root):
        os.makedirs(save_root)
    print('========================')
    print('Preprocessing')
    if not os.path.exists(f'{savetmp}/motion_corrected_data.zarr'):
        if not os.path.exists(savetmp+'/motion_corrected_data_chunks_%03d.zarr'%(num_t_chunks-1)):
            preprocessing(dir_root, [savetmp, save_root], cameraNoiseMat=cameraNoiseMat, nsplit=nsplit, \
                          num_t_chunks=num_t_chunks, dask_tmp=dask_tmp, memory_limit=memory_limit, \
                          is_singlePlane=is_singlePlane, \
                          down_sample_registration=down_sample_registration)

    f = open(savetmp+'processing.tmp', "a")
    f.write(f'Preprocessing finished \n')
    f.close()

    print('========================')
    print('Combining motion corrected data')
    if not os.path.exists(f'{savetmp}/motion_corrected_data.zarr'):
        combine_preprocessing(dir_root, savetmp, num_t_chunks=num_t_chunks, dask_tmp=dask_tmp, memory_limit=memory_limit)
    if not os.path.exists(f'{savetmp}/detrend_data.zarr'):
        detrend_data(dir_root, savetmp, window=baseline_window, percentile=baseline_percentile, \
                     dask_tmp=dask_tmp, memory_limit=memory_limit)
    f = open(savetmp+'processing.tmp', "a")
    f.write(f'Combining motion corrected data finished \n')
    f.close()
    print('========================')
    print('Mask')
    if not os.path.exists(f'{savetmp}/Y_ave.zarr'):
        default_mask(dir_root, savetmp, dask_tmp=dask_tmp, memory_limit=memory_limit)
    f = open(savetmp+'processing.tmp', "a")
    f.write(f'Mask finished \n')
    f.close()
    print('========================')
    print('Demix')
    
    if not os.path.exists(f'{save_root}/Y_max.npy'):
        demix_cells(savetmp, dt, is_skip=is_skip, dask_tmp=dask_tmp, memory_limit=memory_limit)
    f = open(savetmp+'processing.tmp', "a")
    f.write(f'Demix finished \n')
    f.close()
    
    # remove some files --
    f = open(savetmp+'processing.tmp', "a")
    f.write(f'Start moving masks \n')
    f.close()
    if not os.path.exists(f'{save_root}/Y_max.npy'):
        Y_d = da.from_zarr(f'{savetmp}/Y_max.zarr')
        np.save(f'{save_root}/Y_max', Y_d.compute())
    if not os.path.exists(f'{save_root}/Y_d_max.npy'):
        Y_d = da.from_zarr(f'{savetmp}/Y_d_max.zarr')
        np.save(f'{save_root}/Y_d_max', Y_d.compute())
    if not os.path.exists(f'{save_root}/Y_ave.npy'):
        Y_d = da.from_zarr(f'{savetmp}/Y_ave.zarr')
        chunks = Y_d.chunksize[:-1]
        np.save(f'{save_root}/Y_ave', Y_d.compute())
        np.save(f'{save_root}/chunks', chunks)
    
    Y_d = np.load(f'{save_root}/Y_ave.npy')
    chunks = np.load(f'{save_root}/chunks.npy')
    Y_d_max = Y_d.max(axis=0, keepdims=True)
    
    max_ = np.percentile(Y_d_max, mask_max)
    mask_ = Y_d_max>max_
    mask_ = np.repeat(mask_, Y_d.shape[0], axis=0)
    mask_ = da.from_array(mask_, chunks=(1, chunks[1], chunks[2], -1))
        
    print('========================')
    print('DF/F computation')
    f = open(savetmp+'processing.tmp', "a")
    f.write(f'Start dff computation on blocks \n')
    f.close()
    compute_cell_dff_raw(savetmp, mask_, dask_tmp=dask_tmp, memory_limit=0)
    f = open(savetmp+'processing.tmp', "a")
    f.write(f'Start dff computation \n')
    f.close()
    combine_dff(savetmp)
    f = open(savetmp+'processing.tmp', "a")
    f.write(f'Start sparse dff computation \n')
    f.close()
    combine_dff_sparse(savetmp)
    
    return None