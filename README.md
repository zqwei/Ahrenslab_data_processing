# Ahrenslab_data_processing

This repo is for the cell-segment processing of whole-brain zebrafish data in the Ahrens lab.

If one wants his or her data processed, please update the `data_list.csv` file (as follow). The repo will be periodically scanned and the data will be processed when it monitors an update in `data_list.csv`.

## Necessary information in `data_list.csv`
* `dat_dir` : Folder where raw data (in `h5`) is located; the camera infomation file should be placed at the same folder
* `save_dir` : Folder where the processed data is located; please begin with `/nrs/ahrens/Ziqiang/Fish_Datasets/`
* `Processed`: False for unprocessed data
* `cameraNoiseMat`: Camera matrix file -- please contact with weiz // janelia hhmi org for the correct file location.
* `singlePlane` : True: single plane data; False: multiple-plane data
* `baseline_window` : Number of frames to compute baseline -- a reference value is 5 minutes
* `num_t_chunks` : This for the temporal data -- a reference value is the data size (in float64 form in memory)  divided by 200 GB
* `mask` : The threshold for creating brain vs non-brain map
* `dFF` : False for unprocessed data
* `Project` : Project name.

## Contact
weiz // janelia hhmi org
