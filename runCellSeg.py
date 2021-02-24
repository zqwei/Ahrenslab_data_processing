#!/groups/ahrens/home/weiz/miniconda3/envs/myenv/bin/python

import subprocess
from glob import glob
import os, sys
from shutil import rmtree

save_tmp='/scratch/weiz/'


while True:
    try:
        if os.path.exists(save_tmp+'processing.tmp'):
            os.remove(save_tmp+'processing.tmp')
        subprocess.run(['./cellSeg.py'], shell=False, check=True)
    except subprocess.CalledProcessError as e:
        print(e.output)
        # check if denoised_data intermediate files
        denoise_list = sorted(glob(save_tmp+'denoised_data_*.zarr'))
        # check if denoised_data is finished
        if os.path.exists(save_tmp+'denoised_data.zarr'):
            if len(glob(save_tmp+'denoised_data.zarr/'))==0:
                # if not remove the failed denoised_data
                rmtree(save_tmp+'denoised_data.zarr/')
            else:
                # if yes, remove the remaining denoise_list
                if len(denoise_list)>0:
                    for nfile in denoise_list:
                        rmtree(nfile)
        else:
            # if no denoised_data
            # check if the last denoised_data intermediate file succeeds
            if len(glob(denoise_list[-1]+'/*'))==0:
                rmtree(denoise_list[-1])
        if os.path.exists(save_tmp+'processing.tmp'):
            os.remove(save_tmp+'processing.tmp')
        
            
