# Online Speaker Diarization with Speaker ID

## What this is
This directory contains code that adds online speaker diarization and speaker identification functionality to Tilde Online ASR system.

It consists of following two components:
- Voiceprint webservice, that extracts voiceprint from submitted voicesample
- Online ASR pipeline with speaker diarization and ID

## Voiceprint webservice
Voiceprint webservice provides simple API to generate voiceprints from submitted voicesamples.
Later these voiceprints can be passed to online ASR to enable speaker recognition. 

Voiceprint webservice is implemented in the following files:
- master/voiceprint/voiceprint_request_handler.py - voicesample submission handler, adds voiceprint jobs to rabbitmq queue
- master_server.py - main webservice entrypoint 
- voiceprint_worker.py - voiceprint worker, receives jobs from rabbitmq queue
- worker/online/diart_xvector_extractor.py - xvector voiceprint extraction from audio file

## ASR pipeline with online diarization and speaker id
This code integrates online diarization and speaker id with the online ASR.

- worker/online/diart_online_diarization.py - implements online diarization using Diart() and also extends it with speaker identification feature
- worker/online/pipeline_online.py - receives audio stream, performs audio convertation, forwards to ASR and diarization modules. Combines ASR and diarization results together.

The pipeline is used as follows:
```
pipeline=TranscriberOnlinePipeline(config)
pipeline.init_request("newrequest", None)

# if you want to do speaker id, then pass voiceprints to the pipeline
pipeline.init_voiceprints([{"name": "speakerA", "xvector": "..."}, {"name": "speakerB", "xvector": "..."}]) 

# subscribe to results
pipeline.set_full_result_handler(on_full_result)

# pass the audio buffers to pipeline
pipeline.process_data(audio_buffer_as_raw_bytes)

# finalize
pipeline.end_request()
```
