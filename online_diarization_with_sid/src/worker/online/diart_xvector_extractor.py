"""
XVector computation using Diart

Created on May 19th, 2022

@author: Askar Salimbajevs
"""
import logging
from diart.sources import FileAudioSource, RegularAudioFileReader
import diart.blocks as fn
from pyannote.core import SlidingWindow, SlidingWindowFeature
from pyannote.audio.pipelines.utils import PipelineModel
import rx
from rx.scheduler import ImmediateScheduler
from einops import rearrange
import rx.operators as ops
import numpy as np
import torch
import base64

from typing import Optional, Any, Dict, List
from typing import Union, Tuple, Iterable

TemporalFeatures = Union[SlidingWindowFeature, np.ndarray, torch.Tensor]

logger = logging.getLogger(__name__)


class XVectorExtractor(object):

    def __init__(self, conf: Optional[Dict] = None) -> None:
        # Initialize independent modules
        self.segmentation = fn.FramewiseModel(conf.get("segmentation", "pyannote/segmentation"))
        self.embedding = SpeakerEmbedding(conf.get("embedding", "pyannote/embedding"))
        self._sample_rate = conf.get("sample-rate", 16000)
        self._duration = self.segmentation.model.specifications.duration
        self._step = conf.get("step", 0.5)        

    def get_voiceprint_from_file(self, src_file) -> bytes:
        # Generate xvector voiceprint from audio src_file and return as base64 encoded string
        self._voiceprint = None

        # Create audio source from file
        uri = src_file.split(".")[0]
        audio_source = FileAudioSource(
            file=src_file,
            uri=uri,
            reader=RegularAudioFileReaderWithPadding(
                self._sample_rate, self._duration, self._step
            ),
        )

        # Create pipeline
        segmentation_stream = audio_source.stream.pipe(ops.map(self.segmentation))
        # Join audio and segmentation stream to calculate speaker embeddings
        embedding_stream = rx.zip(audio_source.stream, segmentation_stream).pipe(
            ops.starmap(self.embedding),
            ops.reduce(lambda acc, x: acc+x)
        )

        # Use immediate scheduler to block the thread
        embedding_stream.subscribe(on_next=self._on_voiceprint, 
                                   on_error = lambda e: logger.exception("Error {0}".format(e)), 
                                   scheduler=ImmediateScheduler())
        audio_source.read()

        return self._voiceprint

    def _on_voiceprint(self, emb):
        self._voiceprint = base64.b64encode(emb.numpy().tobytes())

class RegularAudioFileReaderWithPadding(RegularAudioFileReader):
    """Reads a file always yielding the same number of samples with a given step.
       Also performs padding to window size
    Parameters
    ----------
    sample_rate: int
        Sample rate of the audio file.
    window_duration: float
        Duration of each chunk of samples (window) in seconds.
    step_duration: float
        Step duration between chunks in seconds.
    """

    def iterate(self, file) -> Iterable[SlidingWindowFeature]:
        waveform, _ = self.audio(file)

        # pad waveform with zeros
        to_pad = self.chunk_loader.window_samples - waveform.shape[1] % self.chunk_loader.window_samples
        import torch.nn.functional as F
        waveform = F.pad(input=waveform, pad=(to_pad, 0), mode='constant', value=0)

        chunks = rearrange(
            waveform.unfold(1, self.chunk_loader.window_samples, self.chunk_loader.step_samples),
            "channel chunk frame -> chunk channel frame",
        ).numpy()
        for i, chunk in enumerate(chunks):
            w = SlidingWindow(
                start=i * self.chunk_loader.step_duration,
                duration=self.resolution,
                step=self.resolution
            )
            yield SlidingWindowFeature(chunk.T, w)


class SpeakerEmbedding:
    """
    Extract speaker embedding given an audio chunk and its segmentation.
    Assumes that the target speaker is dominant in the audio input.
    Parameters
    ----------
    model: pyannote.audio.Model, Text or Dict
        The embedding model. It must take a waveform and weights as input.
    norm: float
        The target norm for the embedding.
        Defaults to 1.
    device: Optional[torch.device]
        The device on which to run the embedding model.
        Defaults to GPU if available or CPU if not.
    """
    def __init__(
        self,
        model: PipelineModel,
        norm: Union[float, torch.Tensor] = 1,
        device: Optional[torch.device] = None,
    ):
        self.embedding = fn.ChunkwiseModel(model, device)
        self.normalize = fn.EmbeddingNormalization(norm)

    def __call__(self, waveform: TemporalFeatures, segmentation: TemporalFeatures) -> torch.Tensor:
        dominant_speaker = np.argmax(np.mean(segmentation.data, axis=0))
        return self.normalize(self.embedding(waveform, segmentation))[dominant_speaker]
