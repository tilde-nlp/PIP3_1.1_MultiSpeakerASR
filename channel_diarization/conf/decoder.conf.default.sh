#!/bin/bash

###
# Default decoder configuration
###

data=data/diarization
diariz_nj=4
thread_string=" --num-threads=4"
nj=1

sad_dir=diariz_voxceleb2_2020/exp/segmentation_1a/tdnn_stats_asr_sad_1a
sad_mfcc_config=diariz_voxceleb2_2020/conf/mfcc_sad.conf
sad_merge_consec=inf

