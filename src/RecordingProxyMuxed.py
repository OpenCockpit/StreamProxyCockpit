# Copyright (C) 2026 by xcentaurix
# License: GNU General Public License v3.0
#
#   RecordingProxyMuxed - serves a single, container-continuous MPEG-TS stream for recording a Pluto TV channel
#   that is already muxed (video and audio in the same transport stream), the RecordingProxyUnmuxed counterpart for a
#   channel LiveProxyUnmuxed doesn't have to mux itself.
#
#   Enigma2's recorder (eServiceMP3Record: uridecodebin straight into a filesink) writes whatever the HLS source
#   delivers to the file, so the recording of a stream with ad breaks (#EXT-X-DISCONTINUITY) ends or becomes
#   unplayable at the first break: the timestamps jump, the continuity counters restart, PIDs and codec
#   parameters may change.
#
#   LiveProxyMuxed already turns the CDN's segments into a ring of container-continuous ones (same PIDs and
#   PAT/PMT, the timeline continued over discontinuities, continuity counters chained) for live playback. This
#   module concatenates them, in order, from the live edge on, into the response of /rec/{channel_id}.ts - like
#   RecordingProxyUnmuxed does with LiveProxyUnmuxed's muxed segments. A channel being watched and recorded at the same time
#   shares one pipeline.
#
#   A stream LiveProxyMuxed can't process (fMP4 or byte-range segments, SAMPLE-AES, ...) is answered with a
#   redirect to the master playlist, so the recorder does exactly what it did before.

import time

from .Debug import logger
from . import LiveProxyMuxed
from .LiveProxyUnmuxed import resolved_master_url

_READ_WAIT = 1.0   # seconds a reader waits for the next segment before checking the channel and the client again
_REOPEN_PAUSE = 2.0       # seconds before a stopped poller is restarted; grows while restarts bring no segment
_REOPEN_PAUSE_MAX = 10.0


def _reopen(channel, state, failures):
    """A new pipeline for the channel whose poller stopped, continuing the old one's timeline.

    The response of a recording never ends because the poller gave up (a stalled playlist, a lost session,
    ...): the recorder (eServiceMP3Record) takes the end of the response for an EOS and starts a new file
    for it, <name>_001.ts. Like a DVB recording without signal, the recording stays open and the file
    simply doesn't grow until segments are back; the timer (the client disconnecting) ends it.
    """
    time.sleep(min(_REOPEN_PAUSE * (1 + failures), _REOPEN_PAUSE_MAX))
    state.last_access = time.monotonic()
    new = LiveProxyMuxed.ensure_channel(channel.channel_id, lambda: resolved_master_url(state), lambda: state.headers,
                                        inherit=channel)
    new.wait_ready(LiveProxyMuxed.START_TIMEOUT)
    logger.debug('%s: muxed poller stopped under a recording (%s), restarted', channel.channel_id, channel.reason or 'ended')
    return new


def _segments(channel, state):
    """Yield the channel's segments in order, starting with the newest one (the live edge)."""
    seq = None
    failures = 0   # restarts of the poller in a row that brought no segment
    while True:
        # the idle reaper of LiveProxyUnmuxed forgets a channel nobody asks for: a recording keeps it (and the poller) alive
        state.last_access = time.monotonic()
        channel.touch()
        item = None
        if seq is None:
            newest = channel.newest_seq()
            if newest is not None:
                seq = newest
            elif not channel.closed:
                time.sleep(0.1)
                continue
        if seq is not None:
            item = channel.wait_for(seq, _READ_WAIT)
        if item is None:
            if channel.closed:
                channel = _reopen(channel, state, failures)
                failures += 1
                seq = None
            continue
        seq, data, _duration = item
        failures = 0
        yield data
        seq += 1


def handle_recording(handler, channel_id, state):
    """Serve /rec/{channel_id}.ts for a muxed channel (LiveProxyUnmuxed.RecordingProxyUnmuxed hands those over)."""
    channel = LiveProxyMuxed.ensure_channel(channel_id, lambda: resolved_master_url(state), lambda: state.headers)
    status = channel.wait_ready(LiveProxyMuxed.START_TIMEOUT)
    if status == 'unsupported':
        logger.debug('%s: muxed recording - redirecting to CDN (%s)', channel_id, channel.reason)
        handler.send_response(302)
        handler.send_header('Location', resolved_master_url(state))
        handler.send_header('Content-Length', '0')
        handler.end_headers()
        return
    if status != 'ok':
        logger.debug('%s: muxed recording - stream not available (%s)', channel_id, channel.reason or status)
        handler.send_response(502)
        handler.end_headers()
        return
    logger.debug('%s: muxed stream - recording through RecordingProxyMuxed', channel_id)
    handler.send_response(200)
    handler.send_header('Content-Type', 'video/MP2T')
    handler.end_headers()
    for chunk in _segments(channel, state):
        handler.wfile.write(chunk)
