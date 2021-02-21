#!/groups/ahrens/home/weiz/miniconda3/envs/myenv/bin/python

import subprocess

try:
    subprocess.run(['./cellSeg.py'], shell=False, check=True)
except subprocess.CalledProcessError as e:
    print(e.output)
