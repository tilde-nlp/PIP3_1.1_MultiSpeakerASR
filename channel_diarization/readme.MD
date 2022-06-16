# Channel diarization

## Introduction
In multi speaker environments speech is often overlapped, as people tend to interrupt one another, adding something to the conversation, not knowing if the other speaker 
has finished, etc. Online communication adds various technical issues (with microphones, speakers, internet connectivity & speed, etc.) to the problem, frequently 
creating confusion and also resulting in an overlapped speech. Speech recognition systems have difficulties processing such audio input, as all speech data is usually
mixed together and there is no information available about the speakers and their turns. There are various methods for speaker diarization, which try to determine what 
speaker spoke when, but they don't always have great accuracy. In online meetings each participant is usually joined from their own device which has its own microphone. 
This allows for audio signal separation into multiple channels where each speaker has its own channel. As any overlaps should happen between channels, overlap issue for 
such multi-channel audio input can be solved by splitting it into channels and processing each channel separately. 

We implemented this idea into Tilde's ASR system. Our ASR pipeline was modified to support such multi-channel audio files, where each audio channel represents a different
speaker. In single-channel (default) mode audio would be downmixed into a single channel audio, then automatically segmented (based on audial pauses) and transcribed; 
each segment has unique speaker ID. In multi-channel mode segments and their transcriptions are generated for each audio channel separately and then combined back
together based on segment timestamps; speaker IDs are equal to the corresponding channel number. 

## Evaluation
To evaluate the implementation, we used the same meeting recording test set, that consists of single-channel online meetings. 
In total, 42 random 30-60s audio clips were extracted from each meeting. 
These audio clips were then segmented into 584 segments, which were manually transcribed and speaker for each segment manually labeled. 
To simulate multi-channel recordings with speaker overlaps, original clips were then transformed into multi-channel clips with random overlaps between channels. 
Iterating through all of the segments from a clip: consecutive segments from the same speaker were joined together; random 0.5-1 second overlap (between current and 
previous speaker) was then generated with a 50% chance of no overlap; joined segments were then added to the channel matching the speaker ID, taking overlap into 
account; finally, metadata timestamps were updated for each segment, to match the adjustments made. 

We tested the performance of channel diarization method on our new multi-channel test set by calculating diarization error rate (DER) and word error rate (WER) metrics
for both multi-channel mode and single-channel mode. Results are displayed in the table below.

|                                          | DER, % | WER, % |
| ---------------------------------------- | ------ | ------ |
| Baseline (single-channel mode)           | 64.97  | 48.69  |
| Channel diarization (multi-channel mode) | 14.34  | 46.51  |

Both error rate metrics show improvements when using channel diarization method. Surprisingly, even though its DER is much lower, the difference in WER is quite small. Manually analyzing the test set and differences between both method results, we observed the following: there are not that many speaker changes in a single clip, channel overlaps were generated only for ~50% of them; there were speech overlaps already in original clips, which made unclear boundaries for some segments, so these overlaps in the source clips were also present in transformed clips. As channel diarization method's advantages are separating overlaps, it would show the best improvements on a test set which have many channel overlaps, without any speech overlaps within a single channel. 
One of potential future work would be making a test set with more speaker overlaps. However, considering that the focus of this project is meetings, and our data set 
was created from real-life online meetings, the speaker change frequency was natural. Furthermore, no speech overlap within a single channel also would not always be 
guaranteed, as there might be other people talking in the same room as meeting participant.

## What this is
This directory contains a bash script that takes multi-channel WAV audio file, performs channel diarization and saves result as Kaldi data directory. 

This scripts requires that Kaldi is installed. You should create "steps" symlink that points to kaldi/egs/wsj/s5/steps and "utils" symlink to kaldi/egs/wsj/s5/utils.
You will also need Chime 6 SAD system from https://kaldi-asr.org/models/m12. 

Then, channel diarization could be executed like, that:
```
./segment_multi_ch_data.sh audio.wav
```

The resulting Kaldi data directory containing diarization result will be placed in "data/diarization".


