#!/bin/bash

# TODO: sampling rates and various SAD and diarization parameters should be configurable

# load decoder conf
source conf/decoder.conf.default.sh;
source conf/decoder.conf.sh;

mfccdir=${data}/feats
data_dir=${data}_seg
cmn_dir=${data}_cmn


# Generate wav.scp, utt2spk, spk2utt & utt2dur
rm -rf $data
mkdir -p $data


file=$1
fbasename="$(basename -- $file)"
fname="${fbasename%.*}"
channel_count=$(sox --i $file | grep "Channels" | cut -d' ' -f 9)
for ((i = 1 ; i <= $channel_count ; i++));
do
	echo "S${i}-$fname S${i}" >> $data/utt2spk
	echo "S${i}-$fname sox $file -t wav - remix $i |" >> $data/wav.scp
	echo "S${i}-$fname  " >> $data/text
done
utils/fix_data_dir.sh $data

# Extract features for SAD
sad_data_dir=${data}_hires
rm -r ${sad_data_dir} || true
utils/copy_data_dir.sh $data $sad_data_dir
steps/make_mfcc.sh --mfcc-config $sad_mfcc_config --nj 1 --cmd utils/run.pl --write-utt2num-frames true \
        ${sad_data_dir} ${sad_data_dir}/make_sad $mfccdir
steps/compute_cmvn_stats.sh ${sad_data_dir} ${sad_data_dir}/make_sad $mfccdir
utils/fix_data_dir.sh ${sad_data_dir}

utils/data/get_utt2dur.sh $sad_data_dir

# segment audio using pretrained SAD neural network
steps/segmentation/detect_speech_activity.sh --cmd utils/run.pl --nj 1 \
	--convert-data-dir-to-whole false --stage 2 \
	--merge_consecutive_max_dur $sad_merge_consec \
	--extra-left-context 79 --extra-right-context 21 \
	--extra-left-context-initial 0 --extra-right-context-final 0 \
	--frames-per-chunk 150 --mfcc-config $sad_mfcc_config \
	$data $sad_dir \
	$mfccdir ${data} ${data}


# Format segments & utt2spk files
rm $data/utt2spk

while read line;
	do
		speaker=$(echo $line | cut -d'-' -f1)
		speaker_id="${speaker:1}"
		t_start=$(echo $line | cut -d'-' -f7)
		t_end=$((echo $line | cut -d'-' -f8) | cut -d' ' -f1)
		t_start_normalized="${t_start:1:4}.${t_start:5:2}0"
		t_end_normalized="${t_end:1:4}.${t_end:5:2}0"
		remainder=$(echo $line | cut -d' ' -f2-)
		echo "S$speaker_id-part_$t_start_normalized-$t_end_normalized $remainder"
		echo "S$speaker_id-part_$t_start_normalized-$t_end_normalized $remainder" >> $data/segments
		echo "S$speaker_id-part_$t_start_normalized-$t_end_normalized $speaker_id" >> $data/utt2spk
		echo "S$speaker_id-part_$t_start_normalized-$t_end_normalized  " >> $data/text
done < $data_dir/segments

utils/utt2spk_to_spk2utt.pl < $data/utt2spk > $data/spk2utt

while read line;
	do
		speaker=$(echo $line | cut -d'-' -f1)
		t_start=$(echo $line | cut -d'-' -f7)
		t_end=$((echo $line | cut -d'-' -f8) | cut -d' ' -f1)
		t_start_normalized="${t_start:1:4}.${t_start:5:2}0"
		t_end_normalized="${t_end:1:4}.${t_end:5:2}0"
		remainder=$(echo $line | cut -d' ' -f2-)
		echo "$speaker-part_$t_start_normalized-$t_end_normalized $remainder" >> $data/feats.scp
done < $data_dir/feats.scp

