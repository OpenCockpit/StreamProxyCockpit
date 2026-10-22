# Copyright (C) 2026 by xcentaurix
# License: GNU General Public License v3.0
#
#   LiveProxyMuxed - the LiveProxyUnmuxed counterpart for streams that are already muxed (video and audio in
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
from concurrent.futures import Future, ThreadPoolExecutor
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
_LIVE_DELAY_SECONDS = 12.0     # a starting player is not told about the newest segments worth this much content
_MAX_HOLD = 12                 # ... but not more than this many segments: 4 left a player that started on one second segments
#                              # with 4s of cushion, the 6s segments that followed then ran it dry (CDN gap of 12.9s)
_HOLD_WINDOW = 2.0             # ... for this many seconds after a player (re)starts: its first playlist(s) only
_SESSION_GAP = 20.0            # a playlist request after this long without one starts a new playback session
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
_JUMP_TICKS = 90000   # a segment starting more than this (1 s) away from where the previous one ended is a timeline jump
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


def _variant_summary(text: str) -> str:
    """The variants of a master playlist as 'bandwidth resolution', highest bandwidth first - the first is the one played."""
    found = []
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith('#EXT-X-STREAM-INF:'):
            continue
        try:
            bandwidth = int(HLSPlaylist.attr_value('BANDWIDTH', line) or 0)
        except ValueError:
            bandwidth = 0
        found.append((bandwidth, HLSPlaylist.attr_value('RESOLUTION', line) or '?'))
    found.sort(key=lambda v: v[0], reverse=True)
    return ', '.join(f'{bandwidth / 1e6:.2f} Mbit/s {resolution}' for bandwidth, resolution in found) or 'none'


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


def _hold_count(durations: 'list[float]') -> int:
    """How many of the newest segments (*durations*, oldest first) a starting player is not told about: enough for
    _LIVE_DELAY_SECONDS of content, at most _MAX_HOLD. hlsdemux starts three segments before the end of the first
    playlist it gets, so that is its cushion: 18s with 6s segments. The CDN publishes its segments irregularly (gaps
    of 5-12s between two, a stall of 16s was seen) and a player that runs dry in such a stall shows black frames and
    a fast catch-up (kernel: 'pts error'). Given the playlist without the newest segments first, the player starts
    that much further back; the next playlist shows them all and the player is that much further behind the newest
    segment for as long as it plays."""
    count = 0
    seconds = 0.0
    for duration in reversed(durations):
        if count >= _MAX_HOLD or seconds >= _LIVE_DELAY_SECONDS:
            break
        count += 1
        seconds += duration if duration > 0 else _DEFAULT_SEG_DURATION
    return count


def _start_count(segments: 'list[Segment]') -> int:
    """How many of the newest *segments* of the CDN playlist a channel starts with: those it hands to the player
    and those it holds back."""
    return _back_count(segments) + _hold_count([seg.duration for seg in segments])


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


_PASSTHROUGH = True   # a source with a stable PID layout is served as the CDN made it, not remuxed


def _stream_layout(ts: bytes) -> 'tuple | None':
    """(PMT pid, PCR pid, ((stream type, pid), ...)) from the first PAT and PMT in *ts*, None if they can't be read."""
    pmt_pid = None
    for off in range(0, min(len(ts) - 187, 188 * 200), 188):
        if ts[off] != 0x47 or not ts[off + 1] & 0x40:
            continue
        afc = (ts[off + 3] >> 4) & 3
        if not afc & 1:
            continue
        pid = ((ts[off + 1] & 0x1F) << 8) | ts[off + 2]
        sec = off + 4 + (1 + ts[off + 4] if afc & 2 else 0)
        sec += 1 + ts[sec]   # pointer field
        if sec + 12 > off + 188:
            continue
        if pid == 0 and pmt_pid is None and ts[sec] == 0:
            end = min(sec + 3 + (((ts[sec + 1] & 0x0F) << 8) | ts[sec + 2]) - 4, off + 188)
            for p in range(sec + 8, end - 3, 4):
                if ts[p] or ts[p + 1]:   # program number 0 is the network PID
                    pmt_pid = ((ts[p + 2] & 0x1F) << 8) | ts[p + 3]
                    break
        elif pmt_pid is not None and pid == pmt_pid and ts[sec] == 2:
            end = sec + 3 + (((ts[sec + 1] & 0x0F) << 8) | ts[sec + 2]) - 4
            if end > off + 188:
                return None
            pcr_pid = ((ts[sec + 8] & 0x1F) << 8) | ts[sec + 9]
            p = sec + 12 + (((ts[sec + 10] & 0x0F) << 8) | ts[sec + 11])
            streams = []
            while p + 5 <= end:
                streams.append((ts[p], ((ts[p + 1] & 0x1F) << 8) | ts[p + 2]))
                p += 5 + (((ts[p + 3] & 0x0F) << 8) | ts[p + 4])
            return (pmt_pid, pcr_pid, tuple(streams)) if streams else None
    return None


_AUDIO_STREAM_TYPES = (0x03, 0x04, 0x0F, 0x11, 0x81)   # MPEG audio, AAC (ADTS, LATM), AC-3
_VIDEO_STREAM_TYPES = (0x01, 0x02, 0x1B, 0x24)          # MPEG video, H.264, H.265


def _frame_ticks(pts_list: list, default: int) -> int:
    """The most common distance between consecutive distinct timestamps (one frame), *default* if there are too few."""
    distinct = sorted(set(pts_list))
    gaps: dict = {}
    for a, b in zip(distinct, distinct[1:]):
        gaps[b - a] = gaps.get(b - a, 0) + 1
    return max(gaps, key=gaps.get) if gaps else default


def _pes_start_pts(ts: bytes, pid: int) -> list:
    """PTS of every PES packet that starts in the packets of *pid* (found with bytes.find, not a loop over all packets)."""
    marker = bytes((0x47, 0x40 | (pid >> 8), pid & 0xFF))
    out = []
    pos = ts.find(marker)
    while pos >= 0:
        if pos % 188 == 0:
            afc = (ts[pos + 3] >> 4) & 3
            pl = pos + 4 + (1 + ts[pos + 4] if afc & 2 else 0)
            if afc & 1 and pl + 14 <= pos + 188 and ts[pl:pl + 3] == b'\x00\x00\x01' and ts[pl + 7] & 0x80:
                b = ts[pl + 9:pl + 14]
                out.append(((b[0] & 0x0E) << 29) | (b[1] << 22) | ((b[2] & 0xFE) << 14) | (b[3] << 7) | ((b[4] & 0xFE) >> 1))
        pos = ts.find(marker, pos + 188)
    return out


_ADTS_RATES = (96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000)


def _last_audio_duration(ts: bytes, pid: int) -> 'int | None':
    """Ticks of audio in the last PES packet of *pid* in *ts*, counted in its AAC (ADTS) frames, None for any other audio.

    A PES packet can hold several audio frames (4 in Rakuten's, 85 ms), the last one of a segment only what is left:
    the end of the audio is then not the start of that packet plus the usual distance between two packets."""
    marker = bytes((0x47, 0x40 | (pid >> 8), pid & 0xFF))
    last = -1
    pos = ts.find(marker)
    while pos >= 0:
        if pos % 188 == 0:
            last = pos
        pos = ts.find(marker, pos + 188)
    if last < 0:
        return None
    payload = bytearray()
    for off in range(last, len(ts) - 187, 188):
        if ts[off] != 0x47 or ((ts[off + 1] & 0x1F) << 8 | ts[off + 2]) != pid:
            continue
        afc = (ts[off + 3] >> 4) & 3
        pl = off + 4 + (1 + ts[off + 4] if afc & 2 else 0)
        if not afc & 1:
            continue
        if off == last:
            if ts[pl:pl + 3] != b'\x00\x00\x01':
                return None
            pl += 9 + ts[pl + 8]
        payload += ts[pl:off + 188]
    frames = ticks = 0
    i = 0
    while i + 7 <= len(payload) and payload[i] == 0xFF and payload[i + 1] & 0xF0 == 0xF0:
        rate = (payload[i + 2] >> 2) & 15
        length = ((payload[i + 3] & 3) << 11) | (payload[i + 4] << 3) | (payload[i + 5] >> 5)
        if rate >= len(_ADTS_RATES) or length < 7:
            break
        frames += 1
        ticks += 1024 * 90000 // _ADTS_RATES[rate]
        i += length
    return ticks if frames else None


def _stream_spans(ts: bytes) -> 'tuple | None':
    """(audio pid, audio (first, last, frame), video (first, last, frame), audio end) of the timestamps in *ts*, None
    unless the segment has one audio and one video stream with timestamps. first/last are the smallest/largest PTS of
    the stream, audio end is the duration of the audio in its last PES packet."""
    layout = _stream_layout(ts)
    if layout is None:
        return None
    apid = next((pid for st, pid in layout[2] if st in _AUDIO_STREAM_TYPES), None)
    vpid = next((pid for st, pid in layout[2] if st in _VIDEO_STREAM_TYPES), None)
    if apid is None or vpid is None:
        return None
    apts = _pes_start_pts(ts, apid)
    vpts = _pes_start_pts(ts, vpid)
    if not apts or not vpts:
        return None
    a_frame = _frame_ticks(apts, 1920)
    return (apid, (min(apts), max(apts), a_frame), (min(vpts), max(vpts), _frame_ticks(vpts, 3003)),
            _last_audio_duration(ts, apid) or a_frame)


_SILENT_ADTS = bytes.fromhex('fff14c8001bffc211004608c1c')   # one silent AAC-LC frame, 48 kHz stereo (ffmpeg's encoder)
_AUDIO_FILL_MAX = 25   # frames (0.53 s): more than this between two segments is not a join of the audio
_AUDIO_SCAN_PACKETS = 2000   # packets of a segment searched for its first audio frame


def _silent_audio_packet(apid: int, pts: int) -> bytes:
    """One TS packet carrying a PES packet with the silent AAC frame, stamped *pts* (adaptation field as stuffing)."""
    pes = bytearray((0x00, 0x00, 0x01, 0xC0, 0, 0, 0x84, 0x80, 0x05))
    pes += bytes((0x21 | (((pts >> 30) & 7) << 1), (pts >> 22) & 0xFF, (((pts >> 15) & 0x7F) << 1) | 1,
                  (pts >> 7) & 0xFF, ((pts & 0x7F) << 1) | 1))
    pes += _SILENT_ADTS
    length = len(pes) - 6
    pes[4], pes[5] = length >> 8, length & 0xFF
    stuffing = 184 - 1 - len(pes)
    pkt = bytearray((0x47, 0x40 | (apid >> 8), apid & 0xFF, 0x30, stuffing))
    pkt += bytes((0x00,)) + b'\xff' * (stuffing - 1)
    pkt += pes
    return bytes(pkt)


def _first_audio_packet(ts: bytes, apid: int) -> 'int | None':
    """Offset of the first packet of the audio PID in *ts* (the PES packet of its first frame starts there)."""
    for off in range(0, min(len(ts) - 187, 188 * _AUDIO_SCAN_PACKETS), 188):
        if ts[off] == 0x47 and ((ts[off + 1] & 0x1F) << 8 | ts[off + 2]) == apid and ts[off + 1] & 0x40:
            return off
    return None


def _insert_silence(ts: bytes, apid: int, first_pts: int, frame_ticks: int, count: int) -> 'bytes | None':
    """*ts* with *count* silent audio frames in front of its first audio frame, stamped from *first_pts* on. None if
    the audio isn't the AAC-LC 48 kHz stereo the silent frame is (its first four ADTS header bytes)."""
    off = _first_audio_packet(ts, apid)
    if off is None:
        return None
    afc = (ts[off + 3] >> 4) & 3
    pl = off + 4 + (1 + ts[off + 4] if afc & 2 else 0)
    es = pl + 9 + ts[pl + 8]
    if ts[es:es + 4] != _SILENT_ADTS[:4]:
        return None
    packets = b''.join(_silent_audio_packet(apid, (first_pts + i * frame_ticks) & _PTS_MASK) for i in range(count))
    return ts[:off] + packets + ts[off:]


def _drop_early_audio(ts: bytes, apid: int, limit: int) -> 'tuple[bytes, int]':
    """*ts* without the audio PES packets at its start whose PTS is before *limit* (their TS packets become null
    packets). Returns (data, number of PES packets dropped)."""
    buf = bytearray(ts)
    dropped = 0
    for off in range(0, len(buf) - 187, 188):
        if buf[off] != 0x47 or ((buf[off + 1] & 0x1F) << 8 | buf[off + 2]) != apid:
            continue
        if buf[off + 1] & 0x40:
            afc = (buf[off + 3] >> 4) & 3
            pl = off + 4 + (1 + buf[off + 4] if afc & 2 else 0)
            if afc & 1 and pl + 14 <= off + 188 and buf[pl:pl + 3] == b'\x00\x00\x01' and buf[pl + 7] & 0x80:
                b = buf[pl + 9:pl + 14]
                pts = ((b[0] & 0x0E) << 29) | (b[1] << 22) | ((b[2] & 0xFE) << 14) | (b[3] << 7) | ((b[4] & 0xFE) >> 1)
                if _signed33(pts - limit) >= 0:
                    break
                dropped += 1
        buf[off + 1] = 0x1F
        buf[off + 2] = 0xFF
        buf[off + 3] = 0x10
        buf[off + 4:off + 188] = b'\xff' * 184
    return bytes(buf), dropped


def _signed33(value: int) -> int:
    value &= _PTS_MASK
    return value - (1 << 33) if value >= 1 << 32 else value


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
        self._a_end = None                            # PTS after the last audio frame sent, shifted
        self._v_end = None                            # the same for video
        self._a_carry = 0                             # ticks the audio is off after the joins so far (+: a hole)
        self._lost = False                            # a segment was lost: treat the next one as a discontinuity
        self._warned_ffmpeg = False
        self._raw_layout = None        # PID layout of the first segment, served unchanged while the others match it
        self._normalize_all = False    # a segment had another layout: remux every one from then on
        self._last_playlist = -math.inf               # when a playlist was last asked for
        self._hold_until = 0.0                        # until when the newest segments are held back from it
        self._start_target = 1                        # segments in the ring before the channel counts as ready
        self._created = time.monotonic()
        if inherit is not None:
            # replaces a poller that stopped under a recorder: continue its timeline and continuity counters, so the
            # recording stays one continuous file; the first segment is a discontinuity of the source
            self._cc = dict(inherit._cc)
            self._delta = inherit._delta
            self._expected = inherit._expected
            self._a_end = inherit._a_end
            self._v_end = inherit._v_end
            self._a_carry = inherit._a_carry
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
        """The media playlist for live playback: the newest segments (not all of them to a starting player)."""
        with self.cond:
            ready = list(self.ready.items())
        if not ready:
            return None
        now = time.monotonic()
        with self.cond:
            if now - self._last_playlist > _SESSION_GAP:
                self._hold_until = now + _HOLD_WINDOW
            self._last_playlist = now
            holding = now < self._hold_until
        hold = _hold_count([duration for _seq, (_data, duration) in ready]) if holding else 0
        hold = min(hold, max(0, len(ready) - self._start_target))   # a cold channel has only the segments the player starts with
        if len(ready) > hold:
            ready = ready[:len(ready) - hold]
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
        # master address (a playout host that redirects to an ad-insertion service whose variants are not on the playout host)
        master_url = getattr(text, 'url', '') or master_url
        if '#EXT-X-STREAM-INF' not in text:
            return master_url
        variants = _variant_urls(text, master_url)
        logger.debug('%s: master playlist variants (highest first, the first is played): %s',
                     self.channel_id, _variant_summary(text))
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
                next_seq = segments[0].seq if media.endlist else segments[max(0, len(segments) - _start_count(segments))].seq
            fresh = []
            preloaded: dict = {}
            if segments:
                if next_seq < segments[0].seq:
                    logger.debug('%s: fell behind the playlist, seq %s -> %s', self.channel_id, next_seq, segments[0].seq)
                    next_seq = segments[0].seq
                    self._lost = True
                fresh = [s for s in segments if s.seq >= next_seq]
            if not self.status:
                # The player is only told about the channel (and given a playlist) when this many segments are ready:
                # those it starts with. With just the first one in the playlist it plays it, and has nothing left when
                # it reloads the playlist a target duration (6s for 5.1s segments) after its first request: a hiccup
                # after about 5s. The segments that are held back from a starting player (_hold_count) are not waited
                # for: it doesn't get them in its first playlist anyway, and they are ready when it reloads.
                self._start_target = max(1, min(len(fresh), _back_count(segments)))
                # download and remux these in parallel (the timeline is rewritten one after the other below)
                preloaded = self._preload(fresh[:self._start_target]) if self._start_target > 1 else {}
            for seg in fresh:
                if self._stop.is_set():
                    return
                self._handle_segment(seg, preloaded.pop(seg.seq, None))
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
                if segments and segments[-1].seq < next_seq - 1 and now - last_progress > _RESYNC_AFTER:
                    # the media sequence went back (a new stream session or ad break numbers from a lower base):
                    # every segment of the playlist is "old" for us and none would ever be fetched again. A playlist
                    # that just stopped growing ends with the segment we handled last (next_seq - 1): that is a CDN
                    # stall, not a rewind, and following it "back" would replay the last few segments
                    next_seq = segments[max(0, len(segments) - _start_count(segments))].seq
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
        """Remux (stream copy) so the PIDs/PAT/PMT are the same for every segment, whatever the source layout.

        -pes_payload_size 0 gives every audio frame its own PES packet and PTS, like the source segments; the muxer's
        default packs about 11 frames (235 ms) under one PTS, which the Dreambox's audio decoder played at a different
        offset from the picture (seen on the unmuxed Pluto path, see LiveProxyUnmuxed._ffmpeg_mux).

        The remux itself changes more than the PIDs (PCR every 0.1 s instead of per frame, the PCR running 0.7 s
        instead of the source's 1.0 s behind the PTS): Hogan's Heroes played extremely out of sync through it, direct
        playback of the same segments in sync. As long as every segment has the PID layout of the first one, the
        segments are therefore served as they came from the CDN (only the timeline and continuity counters are
        continued afterwards, see _continuize)."""
        if _PASSTHROUGH and not self._normalize_all:
            layout = _stream_layout(data)
            if layout is not None:
                if self._raw_layout is None:
                    self._raw_layout = layout
                    logger.debug('%s: stream layout %s | Action: segments are served unchanged', self.channel_id, layout)
                if layout == self._raw_layout:
                    return data
                logger.debug('%s: stream layout changed %s -> %s | Action: every segment is remuxed from now on',
                             self.channel_id, self._raw_layout, layout)
            self._normalize_all = True
        cmd = [_FFMPEG, '-loglevel', 'error', '-probesize', '1000000', '-analyzeduration', '2000000', '-i', 'pipe:0',
               '-map', '0:v:0', '-map', '0:a:0?', '-c', 'copy', '-copyts', '-f', 'mpegts', '-pes_payload_size', '0',
               '-pat_period', '2', 'pipe:1']
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
            disc = disc or _segment_is_disc(ts)
            spans = _stream_spans(ts)
            if spans is not None:
                ts = self._continue_streams(ts, seg, disc, spans)
                ts, self._cc = _patch_continuity(ts, self._cc)
                return ts
            if not disc and self._expected is not None:
                # The CDN tags only some of its timeline jumps (ad clips come with timestamps of their own, one served
                # segment at +130 s, the next one at -5 s, then the programme again at -130 s, none flagged). Played
                # as they were, the decoders lose their sync at each jump and the sound stays off until the next
                # zap (VIDEO0 "pts error", 2.5 s behind the STC, at the end of every ad break).
                gap = _signed33(first + self._delta - self._expected)
                if abs(gap) > _JUMP_TICKS:
                    disc = True
                    logger.debug('%s: segment %s starts %.1fs off the timeline without a discontinuity tag | Action: continued as one',
                                 self.channel_id, seg.seq, gap / 90000)
            if disc and self._expected is not None:
                self._delta = _signed33(self._expected - first)
            ticks = int(seg.duration * 90000) if seg.duration > 0 else int(_DEFAULT_SEG_DURATION * 90000)
            self._expected = (first + self._delta + ticks) & _PTS_MASK
            self._a_end = self._v_end = None
            self._a_carry = 0
            ts = _rewrite_timeline(ts, self._delta)
        ts, self._cc = _patch_continuity(ts, self._cc)
        return ts

    def _continue_streams(self, ts: bytes, seg: Segment, disc: bool, spans: tuple) -> bytes:
        """The timeline of a segment with one audio and one video stream.

        At a discontinuity (tagged by the CDN, or a jump of more than a second that it didn't tag) the segment is
        shifted so that its video continues exactly where the video of the previous one ended; the audio keeps its
        place relative to the picture, as the source has it (an ad clip and the programme start their sound a different
        time before their picture). Where the audio then overlaps the end of the previous segment or leaves a hole
        (80 ms at the end of a break, 39 ms at its start), whole audio frames are dropped or silent ones added, see
        below: the decoder of the box doesn't cut an overlap or wait over a hole, so the sound is late or early from
        then on, a little more with every break."""
        apid, (a_min, a_max, a_frame), (v_min, v_max, v_frame), a_last = spans
        if not disc and self._v_end is not None:
            gap = _signed33(v_min + self._delta - self._v_end)
            if abs(gap) > _JUMP_TICKS:
                disc = True
                logger.debug('%s: segment %s starts %.1fs off the timeline without a discontinuity tag | Action: continued as one',
                             self.channel_id, seg.seq, gap / 90000)
        if self._v_end is None and self._expected is not None:
            self._v_end = self._a_end = self._expected   # the timeline of a segment before streams could be told apart
        if disc and self._v_end is not None:
            self._delta = _signed33(self._v_end - v_min)
        out = _rewrite_timeline(ts, self._delta) if self._delta else ts
        if disc and self._a_end is not None:
            # The decoder of the box plays the audio frames one after the other, whatever their timestamps say: a hole
            # between two segments makes the sound early from then on, an overlap late. The CDN's clips don't join
            # in whole frames (an ad loop overlaps its repeat by 37 ms, the end of a break by 80 ms), so the join is
            # made in whole frames: drop the overlapping ones, add silent ones for a hole, and carry what is left
            # over into the next join, so that the error stays within half a frame instead of adding up.
            gap = _signed33(a_min + self._delta - self._a_end)   # > 0: a hole, < 0: an overlap
            k = int(round(-(gap + self._a_carry) / a_frame))     # audio frames to drop (k > 0) or to add (k < 0)
            added = dropped = 0
            if abs(k) > _AUDIO_FILL_MAX:
                self._a_carry = 0
            else:
                if k > 0:
                    out, dropped = _drop_early_audio(out, apid, a_min + self._delta + k * a_frame - a_frame // 2)
                elif k < 0:
                    try:
                        filled = _insert_silence(out, apid, self._a_end, a_frame, -k)
                    except Exception as exc:   # the join of the audio must never stop the channel
                        logger.debug('%s: segment %s: silent audio frames not added: %r', self.channel_id, seg.seq, exc)
                        filled = None
                    if filled is not None:
                        out, added = filled, -k
                self._a_carry += gap + (dropped - added) * a_frame
            logger.debug('%s: segment %s: audio %s %.0f ms | Action: %s audio frame(s) dropped, %s silent one(s) added, '
                         '%.0f ms carried',
                         self.channel_id, seg.seq, 'hole of' if gap > 0 else 'overlaps the previous one by',
                         abs(gap) / 90, dropped, added, self._a_carry / 90)
        self._v_end = (v_max + self._delta + v_frame) & _PTS_MASK
        self._a_end = (a_max + self._delta + a_last) & _PTS_MASK
        self._expected = self._v_end
        return out

    def _load(self, seg: Segment) -> 'tuple[bytes, float, float] | None':
        """Download, decrypt and remux *seg*: (remuxed data, seconds to fetch, seconds to remux), None if it can't be had."""
        t_start = time.monotonic()
        data = self._fetch_segment(seg)
        if data is None:
            return None
        t_fetched = time.monotonic()
        remuxed = self._normalize(data)
        return remuxed, t_fetched - t_start, time.monotonic() - t_fetched

    def _preload(self, segments: 'list[Segment]') -> 'dict[int, Future]':
        """Start loading *segments* at once. The start of a channel needs several of them and downloading and
        remuxing (ffmpeg, a separate process) one after the other was most of the time it took."""
        pool = ThreadPoolExecutor(max_workers=len(segments), thread_name_prefix=f'MuxedLoad-{self.channel_id[:8]}')
        futures = {seg.seq: pool.submit(self._load, seg) for seg in segments}
        pool.shutdown(wait=False)   # the workers end with their segment
        return futures

    def _handle_segment(self, seg: Segment, preloaded: 'Optional[Future]' = None):
        loaded = preloaded.result() if preloaded is not None else self._load(seg)
        if loaded is None:
            logger.debug('%s: segment %s lost', self.channel_id, seg.seq)
            self._lost = True
            return
        remuxed, fetch_time, remux_time = loaded
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
                         fetch_time, remux_time, t_done - t_remuxed, len(self.ready))
            self._next_seq += 1
            while len(self.ready) > _MAX_READY:
                self.ready.popitem(last=False)
            if not self.status and len(self.ready) >= self._start_target:
                self.status = 'ok'
                logger.debug('%s: muxed channel ready %.2fs after it was created (%s segments)',
                             self.channel_id, time.monotonic() - self._created, len(self.ready))
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
