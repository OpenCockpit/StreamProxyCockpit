# Copyright (C) 2026 by xcentaurix
# License: GNU General Public License v3.0
#
#   LiveProxyMuxed - the LiveProxyUnmuxed counterpart for Pluto TV streams that are already muxed (video and audio in
#   the same transport stream, no #EXT-X-MEDIA:TYPE=AUDIO rendition).
#
#   LiveProxyUnmuxed only has to touch a stream whose audio is a separate rendition; a muxed stream used to be answered
#   with a redirect to the CDN and played by GStreamer as it is. Such a stream is made of independently encoded
#   pieces, though: at every #EXT-X-DISCONTINUITY (an ad starts or ends) the timestamps jump, the continuity
#   counters restart, PIDs and codec parameters may change, and what the decoder makes of that is up to the
#   decoder (distorted video for a few seconds on the dm9xx).
#
#   This module puts the same treatment between the CDN and Enigma2 that LiveProxyUnmuxed gives a demuxed stream:
#
#   - one poller thread per channel (_MuxedChannel) follows the media playlist, fetches (and decrypts) every
#     segment once and remuxes it with ffmpeg (stream copy), so the PIDs and PAT/PMT are the same wherever a
#     piece came from, an ad with a different layout included;
#   - after a discontinuity the timeline (PCR/PTS/DTS) continues where the previous segment ended instead of
#     jumping (_rewrite_timeline, the same approach as RecordingProxyUnmuxed), and the continuity counters are chained
#     over all segments (_patch_continuity);
#   - the result is a ring of container-continuous segments (_MuxedChannel.ready) that is
#     * served as a plain media playlist without #EXT-X-DISCONTINUITY (/mpl/{channel_id}.m3u8,
#       /mseg/{channel_id}/{seq}.ts) for live playback, and
#     * concatenated into one continuous stream by RecordingProxyMuxed for recordings, from the very same ring,
#       so a recording of the channel being watched costs no second download.
#
#   Segments are numbered by this module (never reused), like LiveProxyUnmuxed's chunk_seq, not by the CDN.
#
#   A stream that can't be processed (fMP4 or byte-range segments, SAMPLE-AES, nothing but errors) is flagged
#   unsupported; /auto then redirects to the CDN exactly as before.

import math
import subprocess
import threading
import time
from collections import OrderedDict, namedtuple
from typing import Optional
from urllib.parse import urljoin

from .AES128 import AES128
from .Debug import logger
from .HLSParser import HLSPlaylist
from .LiveProxyUnmuxed import PROXY_HOST, PROXY_PORT, _FFMPEG, _FFMPEG_TIMEOUT, _fetch, _first_pts, _patch_continuity, resolved_master_url
from .RecordingProxyUnmuxed import _PTS_MASK, _rewrite_timeline, _segment_is_disc

_START_BACK_SEGMENTS = 3       # a channel starts at least this many segments before the live edge (hlsdemux does too)
_START_BACK_SECONDS = 9.0      # ... and at least this much content: three 1s segments are no cushion against a CDN that stalls for 4-7s
START_TIMEOUT = 25.0          # seconds until the first segment is ready (or the stream turns out to be unusable)
_PLAYLIST_SEGMENTS = 6         # segments advertised in the served playlist at least
_MIN_TARGET = 3                # the shortest target duration advertised: hlsdemux starts three of them before the end
_MAX_READY = 40                # segments kept per channel; a reader further behind is moved forward
_IDLE_TIMEOUT = 60.0           # seconds without a request or reader before the poller stops
_STALL_LIMIT = 90.0            # seconds without a new segment before the poller gives up
_RESYNC_AFTER = 15.0           # seconds without a new segment, with the playlist behind us, before we follow it back
_STALL_LOG_AFTER = 20.0        # seconds without a new segment before a playlist with nothing new is logged
_STALL_LOG_EVERY = 15.0        # seconds between those log lines
_SEGMENT_TRIES = 3
_SEGMENT_TIMEOUT = 15
_PLAYLIST_TIMEOUT = 8
_SEGMENT_WAIT = 8.0            # how long a segment request waits for a segment that isn't ready yet
_DEFAULT_SEG_DURATION = 5.0
_RESOLVE_RETRY_FAILURES = 3    # consecutive playlist failures before the media playlist url is looked up again

Segment = namedtuple('Segment', 'seq uri duration disc key_uri iv')
MediaPlaylist = namedtuple('MediaPlaylist', 'target segments endlist unsupported')


# ── Playlist parsing ──────────────────────────────────────────────────────────

def _variant_urls(text: str, base: str) -> 'list[str]':
    """Variant playlist urls of a master playlist, highest bandwidth first."""
    variants = []
    lines = [ln.strip() for ln in text.splitlines()]
    for i, line in enumerate(lines):
        if not line.startswith('#EXT-X-STREAM-INF:'):
            continue
        try:
            bandwidth = int(HLSPlaylist.attr_value('BANDWIDTH', line) or 0)
        except ValueError:
            bandwidth = 0
        j = i + 1
        while j < len(lines) and (not lines[j] or lines[j].startswith('#')):
            j += 1
        if j < len(lines):
            variants.append((bandwidth, urljoin(base, lines[j])))
    variants.sort(key=lambda v: v[0], reverse=True)
    return [url for _bw, url in variants]


def _back_count(segments: 'list[Segment]') -> int:
    """How many of the newest *segments* a channel starts with: enough for _START_BACK_SECONDS of content, and at
    least _START_BACK_SEGMENTS. Three segments are only three seconds of content if the stream starts with one
    second segments, and a CDN that stalls for longer leaves the player with nothing to play (a hiccup right after
    the start). Fewer if the playlist doesn't have that many."""
    count = 0
    seconds = 0.0
    for seg in reversed(segments):
        count += 1
        seconds += seg.duration if seg.duration > 0 else _DEFAULT_SEG_DURATION
        if count >= _START_BACK_SEGMENTS and seconds >= _START_BACK_SECONDS:
            break
    return count


def _parse_media(text: str, base: str) -> MediaPlaylist:
    """Segments of a media playlist; whatever this module can't remux is reported in .unsupported."""
    target = 0.0
    seq = 0
    endlist = False
    unsupported = ''
    segments = []
    disc = False
    duration = 0.0
    key_uri = None
    iv = None
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith('#EXT-X-TARGETDURATION:'):
            try:
                target = float(line.split(':', 1)[1])
            except ValueError:
                pass
        elif line.startswith('#EXT-X-MEDIA-SEQUENCE:'):
            try:
                seq = int(line.split(':', 1)[1])
            except ValueError:
                pass
        elif line.startswith('#EXT-X-DISCONTINUITY-SEQUENCE:'):
            pass
        elif line.startswith('#EXT-X-DISCONTINUITY'):
            disc = True
        elif line.startswith('#EXT-X-ENDLIST'):
            endlist = True
        elif line.startswith('#EXT-X-MAP'):
            unsupported = 'fMP4 segments'
        elif line.startswith('#EXT-X-BYTERANGE'):
            unsupported = 'byte-range segments'
        elif line.startswith('#EXT-X-KEY:'):
            method = (HLSPlaylist.attr_value('METHOD', line) or 'NONE').upper()
            if method == 'NONE':
                key_uri = None
                iv = None
            elif method == 'AES-128' and HLSPlaylist.attr_value('URI', line):
                key_uri = urljoin(base, HLSPlaylist.attr_value('URI', line))
                iv_str = HLSPlaylist.attr_value('IV', line)
                iv = bytes.fromhex(iv_str[2:].zfill(32)) if iv_str and iv_str[:2].lower() == '0x' else None
            else:
                unsupported = f'encryption {method}'
        elif line.startswith('#EXTINF:'):
            try:
                duration = float(line.split(':', 1)[1].split(',', 1)[0])
            except ValueError:
                duration = 0.0
        elif not line.startswith('#'):
            segments.append(Segment(seq, urljoin(base, line), duration, disc, key_uri, iv))
            seq += 1
            disc = False
            duration = 0.0
    return MediaPlaylist(target, segments, endlist, unsupported)


# ── Access unit delimiters ────────────────────────────────────────────────────

_AUD_ALL_TYPES = 0xF0   # primary_pic_type 7 ("any slice type") + rbsp trailing bits
_AUD_I_ONLY = 0x10      # primary_pic_type 0 (I slices only) + rbsp trailing bits
_AUD_SCAN_LIMIT = 16384  # bytes of an access unit searched for its first slice NAL


def _find_slice_type(buf: bytearray, start: int) -> int:
    """NAL unit type (1 = non-IDR, 5 = IDR) of the first slice NAL at or after *start*, 0 if there is none (yet)."""
    pos = start
    while True:
        pos = buf.find(b'\x00\x00\x01', pos)
        if pos < 0 or pos + 3 >= len(buf):
            return 0
        nal_type = buf[pos + 3] & 0x1F
        if nal_type in {1, 5}:
            return nal_type
        pos += 3


def _mark_idr_delimiters(ts: bytes) -> bytes:
    """Give the access unit delimiter of every IDR frame the picture type 'I only'.

    ffmpeg's mpegts muxer inserts a delimiter (00 00 00 01 09 F0: any slice type) into every H.264 access unit of
    a stream that has none. Enigma2's recorder stores that byte in the .sc file next to every frame, and its frame
    skipping (eDVBTSTools::findFrame: fast forward/rewind, jumps) only accepts a frame as I-frame if it says "I
    only". With F0 on every frame it never finds one, falls back to byte skipping (an offset estimated from the
    average bitrate) and lands somewhere else in a stream whose bitrate varies, ads included.

    The type is changed in place (the NAL keeps its size). An IDR frame consists of I slices only, so 'I only' is
    true for it; every other frame keeps 'any slice type'. Non-H.264 video is left alone.
    """
    # Only the first packet of each video PES packet (one per frame) is looked at, found with bytes.find at C speed.
    # A loop over every packet of the segment holds the GIL for a long time on the box and slows the main thread.
    vpid = _first_video_pid(ts)
    if vpid is None:
        return ts
    buf = bytearray(ts)
    start_marker = bytes((0x47, 0x40 | (vpid >> 8), vpid & 0xFF))   # sync byte, payload unit start, the video pid
    count = len(buf)
    pos = buf.find(start_marker)
    while pos >= 0:
        if pos % 188:   # the marker bytes happened to appear inside a packet
            pos = buf.find(start_marker, pos + 1)
            continue
        afc = (buf[pos + 3] >> 4) & 3
        pl = pos + 4 + (1 + buf[pos + 4] if afc & 2 else 0)
        if afc & 1 and pl + 9 < pos + 188 and buf[pl:pl + 3] == b'\x00\x00\x01':
            es = pl + 9 + buf[pl + 8]   # start of the elementary stream data
            if buf[es:es + 4] == b'\x00\x00\x00\x01':
                nal = es + 4
            elif buf[es:es + 3] == b'\x00\x00\x01':
                nal = es + 3
            else:
                nal = 0
            if nal and nal + 2 <= pos + 188 and buf[nal] & 0x1F == 9 and buf[nal + 1] == _AUD_ALL_TYPES:
                # the first slice NAL of the access unit decides, it follows within a packet or two
                au_data = bytearray(buf[nal + 2:pos + 188])
                nal_type = _find_slice_type(au_data, 0)
                nxt = pos + 188
                while not nal_type and len(au_data) <= _AUD_SCAN_LIMIT and nxt + 188 <= count:
                    if buf[nxt] == 0x47 and ((buf[nxt + 1] & 0x1F) << 8 | buf[nxt + 2]) == vpid:
                        if buf[nxt + 1] & 0x40:   # the next access unit already
                            break
                        afc2 = (buf[nxt + 3] >> 4) & 3
                        if afc2 & 1:
                            au_data += buf[nxt + 4 + (1 + buf[nxt + 4] if afc2 & 2 else 0):nxt + 188]
                            nal_type = _find_slice_type(au_data, 0)
                    nxt += 188
                if nal_type == 5:
                    buf[nal + 1] = _AUD_I_ONLY
        pos = buf.find(start_marker, pos + 188)
    return bytes(buf)


def _first_video_pid(ts: bytes) -> 'int | None':
    """The pid of the first video PES packet (stream id 0xE0-0xEF) within the first 400 packets of *ts*."""
    for off in range(0, min(len(ts) - 187, 188 * 400), 188):
        if ts[off] != 0x47 or not ts[off + 1] & 0x40:
            continue
        afc = (ts[off + 3] >> 4) & 3
        pl = off + 4 + (1 + ts[off + 4] if afc & 2 else 0)
        if afc & 1 and pl + 4 <= off + 188 and ts[pl:pl + 3] == b'\x00\x00\x01' and 0xE0 <= ts[pl + 3] <= 0xEF:
            return ((ts[off + 1] & 0x1F) << 8) | ts[off + 2]
    return None


# ── The per-channel pipeline ──────────────────────────────────────────────────

class _MuxedChannel:
    """Follows one muxed channel and keeps a ring of container-continuous segments."""

    def __init__(self, channel_id: str, master_url_getter, headers_getter=None, inherit: 'Optional[_MuxedChannel]' = None):
        self.channel_id = channel_id
        self._master_url_getter = master_url_getter   # current master url (it may be refreshed under us)
        self._headers_getter = headers_getter         # current request headers of the channel (dict), or None
        self.cond = threading.Condition()
        self.ready: 'OrderedDict[int, tuple[bytes, float]]' = OrderedDict()   # served seq -> (data, duration)
        self._next_seq = 0                            # served numbering, never reused
        self.status = ''                              # '', 'ok', 'unsupported' or 'error'
        self.reason = ''
        self.closed = False
        self.last_access = time.monotonic()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name=f'MuxedPoller-{channel_id[:8]}')
        self._keys: dict = {}
        self._cc: dict = {}                           # continuity counter chain
        self._delta = 0                               # timeline shift in effect
        self._expected = None                         # PTS the next segment should start at
        self._lost = False                            # a segment was lost: treat the next one as a discontinuity
        self._warned_ffmpeg = False
        self._start_target = 1                        # segments in the ring before the channel counts as ready
        if inherit is not None:
            # replaces a poller that stopped under a recorder: continue its timeline and continuity counters, so the
            # recording stays one continuous file; the first segment is a discontinuity of the source
            self._cc = dict(inherit._cc)
            self._delta = inherit._delta
            self._expected = inherit._expected
            self._lost = True

    # -- public --

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()

    def touch(self):
        self.last_access = time.monotonic()

    def wait_ready(self, timeout: float) -> str:
        """Block until the first segment is ready or the stream turned out to be unusable; returns the status."""
        end = time.monotonic() + timeout
        with self.cond:
            while not self.status and not self.closed:
                left = end - time.monotonic()
                if left <= 0:
                    break
                self.cond.wait(left)
            return self.status or ('error' if self.closed else '')

    def wait_for(self, seq: int, timeout: float) -> 'tuple[int, bytes, float] | None':
        """(seq, data, duration) of segment *seq*, or of the oldest one if *seq* is gone already. Waits up to
        *timeout* seconds for a segment that isn't there yet; None if it doesn't arrive or the channel closed."""
        end = time.monotonic() + timeout
        with self.cond:
            while True:
                if self.ready:
                    first = next(iter(self.ready))
                    seq = max(seq, first)
                    if seq in self.ready:
                        data, duration = self.ready[seq]
                        return seq, data, duration
                if self.closed:
                    return None
                left = end - time.monotonic()
                if left <= 0:
                    return None
                self.cond.wait(left)

    def newest_seq(self) -> 'int | None':
        with self.cond:
            return next(reversed(self.ready)) if self.ready else None

    def playlist(self) -> 'str | None':
        """The media playlist for live playback: the newest segments, no discontinuity tags."""
        with self.cond:
            ready = list(self.ready.items())
        if not ready:
            return None
        items = ready[-_PLAYLIST_SEGMENTS:]
        # Not the longest segment rounded up, but one second less (and three at least): hlsdemux reloads the playlist
        # once per target duration, plus its own overhead (about 9 ms per reload in the logs). With a target of 6s for
        # 6s segments it fetched a segment every 6.009s, 0.15% slower than the decoder plays: the buffer ran dry
        # after about 4 minutes and the audio decoder then dropped frames (tsm fail) at every segment. With a shorter
        # target it polls faster than segments appear and fetches each as soon as it is there. hlsdemux also starts
        # three target durations before the end of the playlist, so that is what a start has as a cushion: three
        # seconds only, with the one second segments some channels start with (the CDN then stalled for 4-7s and the
        # playback hiccuped right after the start), hence the minimum.
        target = max(_MIN_TARGET, math.ceil(max(duration for _seq, (_data, duration) in items)) - 1)
        # offer more than the 6 segments if they are short, so that those three targets are in the playlist
        total = sum(duration for _seq, (_data, duration) in items)
        first = len(ready) - len(items)
        while first > 0 and total < 4 * target:
            first -= 1
            total += ready[first][1][1]
        items = ready[first:]
        target = max(target, math.ceil(max(duration for _seq, (_data, duration) in items)) - 1)
        out = ['#EXTM3U', '#EXT-X-VERSION:3', f'#EXT-X-TARGETDURATION:{target}', f'#EXT-X-MEDIA-SEQUENCE:{items[0][0]}']
        for seq, (_data, duration) in items:
            out.append(f'#EXTINF:{duration:.3f},')
            out.append(f'http://{PROXY_HOST}:{PROXY_PORT}/mseg/{self.channel_id}/{seq}.ts')
        return '\n'.join(out) + '\n'

    # -- internals --

    def _set_status(self, status: str, reason: str = ''):
        with self.cond:
            if not self.status:
                self.status = status
                self.reason = reason
            self.cond.notify_all()

    def _run(self):
        try:
            self._loop()
        except Exception as exc:  # pylint: disable=broad-except
            logger.debug('%s: muxed poller failed: %s', self.channel_id, exc)
            self._set_status('error', str(exc))
        finally:
            with self.cond:
                self.closed = True
                self.cond.notify_all()
            with _lock:
                if _channels.get(self.channel_id) is self:
                    _channels.pop(self.channel_id, None)
            logger.debug('%s: muxed poller stopped', self.channel_id)

    def _headers(self) -> 'dict | None':
        return self._headers_getter() if self._headers_getter else None

    def _media_url(self) -> 'str | None':
        master_url = self._master_url_getter()
        text = _fetch(master_url, timeout=_PLAYLIST_TIMEOUT, tag='muxed:master', headers=self._headers()) if master_url else None
        if not text or not text.lstrip().startswith('#EXTM3U'):
            return None
        # relative addresses are relative to where the playlist was finally fetched from, after the redirects of the
        # master address (Rakuten TV: playout host -> AWS MediaTailor, whose variants are not on the playout host)
        master_url = getattr(text, 'url', '') or master_url
        if '#EXT-X-STREAM-INF' not in text:
            return master_url
        variants = _variant_urls(text, master_url)
        return variants[0] if variants else None

    def _loop(self):
        media_url = None
        for _attempt in range(3):
            media_url = self._media_url()
            if media_url:
                break
            self._stop.wait(1.0)
        if not media_url:
            self._set_status('error', 'master playlist not reachable')
            return
        next_seq = None
        last_progress = time.monotonic()
        last_stall_log = 0.0
        failures = 0
        while not self._stop.is_set():
            if time.monotonic() - self.last_access > _IDLE_TIMEOUT:
                logger.debug('%s: muxed channel idle, stopping', self.channel_id)
                return
            text = _fetch(media_url, timeout=_PLAYLIST_TIMEOUT, tag='muxed:playlist', headers=self._headers())
            if not text:
                failures += 1
                if failures >= _RESOLVE_RETRY_FAILURES:
                    media_url = self._media_url() or media_url
                    failures = 0
                if time.monotonic() - last_progress > _STALL_LIMIT:
                    logger.debug('%s: muxed playlist not reachable for %ss, giving up', self.channel_id, int(_STALL_LIMIT))
                    self._set_status('error', 'playlist not reachable')
                    return
                self._stop.wait(1.0)
                continue
            failures = 0
            media = _parse_media(text, media_url)
            if media.unsupported:
                logger.debug('%s: muxed stream not processable (%s)', self.channel_id, media.unsupported)
                self._set_status('unsupported', media.unsupported)
                return
            segments = media.segments
            if next_seq is None and segments:
                next_seq = segments[0].seq if media.endlist else segments[max(0, len(segments) - _back_count(segments))].seq
            fresh = []
            if segments:
                if next_seq < segments[0].seq:
                    logger.debug('%s: fell behind the playlist, seq %s -> %s', self.channel_id, next_seq, segments[0].seq)
                    next_seq = segments[0].seq
                    self._lost = True
                fresh = [s for s in segments if s.seq >= next_seq]
            if not self.status:
                # The player is only told about the channel (and given a playlist) when this many segments are ready.
                # With just the first one in the playlist it plays it, and has nothing left when it reloads the
                # playlist a target duration (6s for 5.1s segments) after its first request: a hiccup after about 5s.
                self._start_target = max(1, min(len(fresh), _back_count(segments)))
            for seg in fresh:
                if self._stop.is_set():
                    return
                self._handle_segment(seg)
                next_seq = seg.seq + 1
                last_progress = time.monotonic()
            if not self.status and self.ready:
                self._set_status('ok')   # fewer segments than planned (one was lost): don't wait for more
            if media.endlist and (not segments or next_seq > segments[-1].seq):
                logger.debug('%s: end of muxed playlist', self.channel_id)
                return
            if not fresh:
                now = time.monotonic()
                if segments and now - last_progress > _STALL_LOG_AFTER and now - last_stall_log > _STALL_LOG_EVERY:
                    last_stall_log = now
                    logger.debug('%s: nothing new in the playlist for %ss: seq %s..%s, waiting for %s',
                                 self.channel_id, int(now - last_progress), segments[0].seq, segments[-1].seq, next_seq)
                if segments and segments[-1].seq < next_seq and now - last_progress > _RESYNC_AFTER:
                    # the media sequence went back (a new stitcher session or ad break numbers from a lower base):
                    # every segment of the playlist is "old" for us and none would ever be fetched again
                    next_seq = segments[max(0, len(segments) - _back_count(segments))].seq
                    self._lost = True
                    last_progress = now
                    logger.debug('%s: media sequence went back to %s..%s, following it from %s',
                                 self.channel_id, segments[0].seq, segments[-1].seq, next_seq)
                    continue
                if now - last_progress > _STALL_LIMIT:
                    logger.debug('%s: no new muxed segment for %ss, giving up', self.channel_id, int(_STALL_LIMIT))
                    return
                self._stop.wait(max(0.5, min((media.target or _DEFAULT_SEG_DURATION) / 2.0, 3.0)))

    def _fetch_segment(self, seg: Segment) -> 'bytes | None':
        for attempt in range(_SEGMENT_TRIES):
            if self._stop.is_set():
                return None
            data = _fetch(seg.uri, binary=True, timeout=_SEGMENT_TIMEOUT, tag='muxed:segment', headers=self._headers())
            if data and seg.key_uri:
                key = self._keys.get(seg.key_uri) or AES128.fetch_key(seg.key_uri, lambda url, **kwargs: _fetch(url, headers=self._headers(), **kwargs))
                if not key:
                    data = None
                else:
                    self._keys[seg.key_uri] = key
                    data = AES128.decrypt(data, key, AES128.iv(seg.iv, seg.seq))
            if data:
                if data[0] == 0x47:
                    return data
                logger.debug('%s: segment %s is not MPEG-TS (%s bytes)', self.channel_id, seg.seq, len(data))
                return None
            logger.debug('%s: segment %s fetch %s failed', self.channel_id, seg.seq, attempt + 1)
            self._stop.wait(0.5)
        return None

    def _normalize(self, data: bytes) -> bytes:
        """Remux (stream copy) so the PIDs/PAT/PMT are the same for every segment, whatever the source layout."""
        cmd = [_FFMPEG, '-loglevel', 'error', '-probesize', '1000000', '-analyzeduration', '2000000', '-i', 'pipe:0',
               '-map', '0:v:0', '-map', '0:a:0?', '-c', 'copy', '-copyts', '-f', 'mpegts', '-pat_period', '2', 'pipe:1']
        try:
            r = subprocess.run(cmd, input=data, capture_output=True, timeout=_FFMPEG_TIMEOUT, check=False)
            if r.returncode == 0 and r.stdout:
                return r.stdout
            logger.debug('%s: ffmpeg rc=%s: %s', self.channel_id, r.returncode, r.stderr.decode(errors='replace')[-200:])
        except (OSError, subprocess.SubprocessError) as exc:
            if not self._warned_ffmpeg:
                self._warned_ffmpeg = True
                logger.debug('%s: ffmpeg failed: %s', self.channel_id, exc)
        return data  # better the raw segment than a hole

    def _continuize(self, ts: bytes, seg: Segment, disc: bool) -> bytes:
        """Keep the timeline and the continuity counters running over a discontinuity."""
        first = _first_pts(ts)
        if first is not None:
            if (disc or _segment_is_disc(ts)) and self._expected is not None:
                delta = (self._expected - first) & _PTS_MASK
                self._delta = delta - (1 << 33) if delta >= 1 << 32 else delta   # signed, 33-bit
            ticks = int(seg.duration * 90000) if seg.duration > 0 else int(_DEFAULT_SEG_DURATION * 90000)
            self._expected = (first + self._delta + ticks) & _PTS_MASK
            ts = _rewrite_timeline(ts, self._delta)
        ts, self._cc = _patch_continuity(ts, self._cc)
        return ts

    def _handle_segment(self, seg: Segment):
        t_start = time.monotonic()
        data = self._fetch_segment(seg)
        if data is None:
            logger.debug('%s: segment %s lost', self.channel_id, seg.seq)
            self._lost = True
            return
        t_fetched = time.monotonic()
        remuxed = self._normalize(data)
        t_remuxed = time.monotonic()
        ts = _mark_idr_delimiters(remuxed)
        disc = seg.disc or self._lost
        self._lost = False
        if disc:
            logger.debug('%s: segment %s starts after a discontinuity', self.channel_id, seg.seq)
        out = self._continuize(ts, seg, disc)
        duration = seg.duration if seg.duration > 0 else _DEFAULT_SEG_DURATION
        t_done = time.monotonic()
        with self.cond:
            self.ready[self._next_seq] = (out, duration)
            logger.debug('%s: segment %s (cdn %s, %.1fs, %s bytes) ready: fetch %.2fs, remux %.2fs, rewrite %.2fs, %s in the ring',
                         self.channel_id, self._next_seq, seg.seq, duration, len(out),
                         t_fetched - t_start, t_remuxed - t_fetched, t_done - t_remuxed, len(self.ready))
            self._next_seq += 1
            while len(self.ready) > _MAX_READY:
                self.ready.popitem(last=False)
            if not self.status and len(self.ready) >= self._start_target:
                self.status = 'ok'
            self.cond.notify_all()


# ── Channel registry ──────────────────────────────────────────────────────────

_lock = threading.Lock()
_channels: 'dict[str, _MuxedChannel]' = {}


def get_channel(channel_id: str) -> '_MuxedChannel | None':
    with _lock:
        channel = _channels.get(channel_id)
    if channel is not None:
        channel.touch()
    return channel


def ensure_channel(channel_id: str, master_url_getter, headers_getter=None, inherit: 'Optional[_MuxedChannel]' = None) -> _MuxedChannel:
    """The running pipeline of *channel_id*, started if there is none (idempotent, like register_channel).

    *inherit*: the pipeline this one replaces; a new one continues its timeline (see _MuxedChannel)."""
    with _lock:
        channel = _channels.get(channel_id)
        if channel is None or channel.closed:
            channel = _MuxedChannel(channel_id, master_url_getter, headers_getter, inherit)
            _channels[channel_id] = channel
            channel.start()
    channel.touch()
    return channel


def close_channel(channel_id: str) -> bool:
    """Stop the pipeline of one channel (the live slot moved on); the entry goes away when the poller exits."""
    with _lock:
        channel = _channels.get(channel_id)
    if channel is None:
        return False
    channel.stop()
    return True


# ── HTTP handlers (called from LiveProxyUnmuxed.HLSProxyHandler) ─────────────────────

def _redirect(handler, location: str):
    handler.send_response(302)
    handler.send_header('Location', location)
    handler.send_header('Content-Length', '0')
    handler.end_headers()


def handle_auto(handler, channel_id: str, state):
    """/auto for a channel without a separate audio rendition: start the pipeline and point the player at it, or
    at the CDN as before if the stream can't be processed."""
    channel = ensure_channel(channel_id, lambda: resolved_master_url(state), lambda: state.headers)
    status = channel.wait_ready(START_TIMEOUT)
    if status == 'ok':
        logger.debug('%s: muxed stream - served through LiveProxyMuxed', channel_id)
        _redirect(handler, f'http://{PROXY_HOST}:{PROXY_PORT}/mpl/{channel_id}.m3u8')
    elif status == 'unsupported':
        logger.debug('%s: muxed stream - redirecting to CDN (%s)', channel_id, channel.reason)
        _redirect(handler, resolved_master_url(state))
    else:
        handler._send(502, b'Bad gateway', 'text/plain')


def handle_playlist(handler, channel_id: str):
    channel = get_channel(channel_id)
    text = channel.playlist() if channel is not None else None
    if text is None:
        handler._send(404, b'Unknown channel', 'text/plain')
        return
    seqs = [int(line.rsplit('/', 1)[1].split('.')[0]) for line in text.splitlines() if '/mseg/' in line]
    logger.debug('%s: playlist offered: segments %s-%s', channel_id, seqs[0] if seqs else '-', seqs[-1] if seqs else '-')
    handler._send(200, text.encode(), 'application/x-mpegURL')


def handle_segment(handler, channel_id: str, seq: int):
    channel = get_channel(channel_id)
    started = time.monotonic()
    item = channel.wait_for(seq, _SEGMENT_WAIT) if channel is not None else None
    waited = time.monotonic() - started
    if item is None:
        logger.debug('%s: segment %s requested, not available after %.2fs', channel_id, seq, waited)
        handler._send(404, b'Segment not available', 'text/plain')
        return
    logger.debug('%s: segment %s requested, served after waiting %.2fs (%s bytes)', channel_id, seq, waited, len(item[1]))
    handler._send(200, item[1], 'video/MP2T')
