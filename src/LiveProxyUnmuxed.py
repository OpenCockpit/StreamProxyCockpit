# Copyright (C) 2026 by xcentaurix
# License: GNU General Public License v3.0
#
#   LiveProxyUnmuxed – local HLS proxy that converts demuxed streams
#   (separate video-TS + audio-TS delivered via EXT-X-MEDIA:TYPE=AUDIO) into
#   single muxed TS segments that Enigma2's DVB hardware pipeline handles.


import bisect
import os
import subprocess
import tempfile
import threading
import time
from collections import OrderedDict, namedtuple
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import urljoin, urlparse, parse_qs
from concurrent.futures import Future, ThreadPoolExecutor

import requests

from .Debug import logger
from .HLSParser import HLSPlaylist
from .AES128 import AES128

PROXY_HOST = '127.0.0.1'
PROXY_PORT = 7654
_SEG_DURATION = 5.0            # fallback segment duration (s) used before the first playlist
#                              # parse; overridden per-channel from #EXTINF in _ChannelState.
_MAX_SEG_CACHE = 200           # chunk_seq entries kept per channel in state._ready_segs /
#                              # state._window_meta (produced output windows, keyed by this
#                              # proxy's own monotonic counter - not a CDN-seq-keyed cache)
_SEQ_REPROCESS_WINDOW = 10     # how far behind the current base seq the video-prefetch loop's
#                              # fetched_seqs/bumped_disc_seqs and the audio-prefetch loop's/
#                              # _append_audio's own CDN-media-sequence-keyed dedup sets
#                              # (state._audio_disc_seqs_seen, state._audio_appended_seqs)
#                              # forget a seq, letting it be treated as brand new if it
#                              # reappears. These must forget seqs on this same short horizon,
#                              # not _MAX_SEG_CACHE's much wider one: some channels serve
#                              # a stitched ad-pod/filler loop whose playlist media-sequence
#                              # numbers wrap back down and repeat rather than growing
#                              # monotonically forever, and once a seq reappears its
#                              # #EXT-X-DISCONTINUITY flag must be re-evaluated rather than
#                              # silently reusing whatever was decided the first time that seq
#                              # number was seen. Confirmed in production (pre-windowed-mux
#                              # design, when this dedup lived in assign_audio_epoch): at
#                              # _MAX_SEG_CACHE width it kept returning a long-stale epoch for a
#                              # repeating seq while video_epoch (whose own dedup already used
#                              # this narrow window) correctly kept advancing across each real
#                              # repeated transition - video_epoch ran 3+ epochs ahead of
#                              # audio_epoch every loop, leaving most of the channel's audio
#                              # muxed with silence for minutes at a stretch. Note: this window
#                              # is specifically for CDN-media-sequence-keyed sets: chunk_seq
#                              # (this proxy's own monotonic counter) never wraps or repeats, so
#                              # nothing keyed by chunk_seq needs - or should use - this kind of
#                              # age-based pruning; see mark_provisional's own comment.
_PREFETCH_LEAD_SEGMENTS = 20   # how far ahead of the last segment Enigma2 actually
#                              # requested the video/audio pre-fetch threads are allowed to
#                              # run. A no-op for live: the CDN only ever publishes new
#                              # segments at the live rate to begin with, so the pre-fetch
#                              # threads can never get anywhere near this far ahead of real
#                              # playback regardless. It matters for VOD, whose entire
#                              # playlist (however long the asset is) is visible from the very
#                              # first poll - without this bound both pre-fetch threads race
#                              # to mux/fetch the whole asset as fast as network+CPU allow,
#                              # observed in production climbing to 150+ segments ahead of
#                              # what Enigma2 had actually played, which starves the *audio*
#                              # ring of the near-term content it actually needs (evicted by
#                              # the size cap in favor of far-future entries) long before
#                              # video gets anywhere near using them.
_VOD_KEEP_BEHIND_SEGMENTS = 24  # VOD only: already-played segments kept behind the last one Enigma2
#                              # requested (~2 min), enough for a retried request or a short rewind.
#                              # A VOD asset is not a ring like a live channel, so without this every
#                              # produced segment (~1.5 MB each) stays in RAM up to _MAX_SEG_CACHE -
#                              # about 300 MB, on a receiver with 1 GB and no swap.
_MAX_UNCONSUMED_AUDIO_EPOCHS = 8  # closed audio epochs kept regardless of age/count while
#                              # video hasn't consumed them yet - see _prune_audio_bufs
_AUDIO_BUFFER_RETAIN_EPOCHS = 1  # closed _RenditionBuffer epochs kept, beyond the current
#                              # one, purely so _slice_audio_for_window's one-epoch-back
#                              # fallback (the audio rendition's own disc tag landing a
#                              # segment or two late relative to video's) has something to
#                              # search - matches the exact fallback depth pop_audio_for_pts
#                              # used to allow via its own allow_stale_epoch gate.
_MAX_AUDIO_EPOCH_LAG = 2       # how far behind (or ahead) video_epoch and audio_epoch may
#                              # drift, checked in _cut_and_mux_next_window whenever a
#                              # window's audio search misses, before treating it as a real,
#                              # persistent divergence worth forcibly resyncing rather than
#                              # the ordinary one-epoch tag-timing skew _AUDIO_BUFFER_RETAIN_
#                              # EPOCHS's own fallback already rides out. Distinct from that
#                              # constant on purpose: this one is a resync *trigger*
#                              # threshold, not a buffer *retention* depth - conflating the
#                              # two (relying on retention depth alone, with no resync
#                              # mechanism at all) was confirmed in production as a
#                              # permanent, unrecoverable silence outage the moment the two
#                              # counters drifted apart by this much, with nothing left to
#                              # bring them back in sync on their own. >=, not >: a gap that
#                              # stabilizes at exactly this value (rather than growing past
#                              # it) must still trigger the resync, or it can sit there
#                              # forever without ever crossing a strictly-greater-than check.
_DISC_LOOKAHEAD_SEGMENTS = 30  # VOD only: how far ahead, within the current poll's
#                              # already-fetched playlist, _video_prefetch_loop looks for an
#                              # upcoming #EXT-X-DISCONTINUITY before letting
#                              # _mux_whole_video_segment's "audio_epoch already ahead of
#                              # video_epoch" resync fire on a miss. Ported from the old
#                              # (pre-windowed-mux) design's pending_disc_ahead, which existed
#                              # for exactly this race: a segment right before a real
#                              # transition can miss its audio (the audio rendition's own disc
#                              # tag landing first, rolling its buffer over to the next epoch
#                              # before this segment's search concludes) and get misread as
#                              # "video missed its own marker" - triggering a resync bump -
#                              # when the video rendition's own genuine tag was simply about to
#                              # arrive a segment or two later in this same pass. Without this
#                              # guard the two bumps stack (resync, then the real tag) and
#                              # video_epoch ends up one ahead of where audio actually is for
#                              # the rest of the session - confirmed in production as exactly
#                              # that: video_epoch overshot by 1, permanently below
#                              # _MAX_AUDIO_EPOCH_LAG's own resync-down trigger, papered over
#                              # afterward only by the stale-epoch fallback rather than a clean
#                              # resync, and losing the one window right at the transition (and
#                              # the disc-introducing window itself) to silence in the meantime.
#                              # The pending tag's own ordinary is_disc bump, moments later in
#                              # this same loop, is what's actually supposed to supply the
#                              # correction - not this one. Whole-playlist visibility for VOD
#                              # (the entire remaining asset is already in this poll's all_discs,
#                              # regardless of _PREFETCH_LEAD_SEGMENTS pacing) is what makes this
#                              # a plain, synchronous list lookup rather than a real wait.
_MAX_AUDIO_BUFFER_BYTES = 50_000_000  # hard per-buffer size backstop, checked in
#                              # _prune_audio_bufs regardless of match success or closed
#                              # status. trim_before() only ever shrinks a buffer in response
#                              # to a *successful* cut - a buffer that never matches anything
#                              # (the exact scenario _MAX_AUDIO_EPOCH_LAG's resync exists to
#                              # prevent) would otherwise grow without bound for as long as
#                              # its epoch's prefetch thread keeps appending to it. Should
#                              # essentially never trigger with that resync in place; sized
#                              # generously (roughly the old fixed-size audio ring's own
#                              # worst-case footprint, _MAX_AUDIO_RING x a typical ~1-2MB
#                              # segment) so it only ever bites as a true backstop, not a
#                              # routine eviction path competing with trim_before's own.
_VIDEO_BUFFER_RETAIN_MARGIN_BYTES = 400000  # bytes of already-cut video kept behind the
#                              # trim point in the current _RenditionBuffer, so the narrowed
#                              # on-demand fallback in _handle_segment can still re-derive a
#                              # just-produced window without a fresh CDN re-fetch.
_AUDIO_FETCH_WORKERS = 4       # parallel audio segment fetch threads
_AUDIO_PREPOPULATE_CAP = 10    # max segments _handle_playlist's synchronous first-request
#                              # pre-populate pass fetches. Deliberately well below
#                              # _MAX_AUDIO_RING (unlike the old cap, which was
#                              # _MAX_AUDIO_RING itself): that left the ring already at
#                              # 100% capacity, with nothing yet consumed, before either
#                              # pre-fetch thread had even started - so the very next
#                              # segment _audio_prefetch_loop added (still within its own
#                              # _PREFETCH_LEAD_SEGMENTS bound) evicted real, not-yet-played
#                              # audio instead of something video had already used. Observed
#                              # in production on VOD as steady mid-playback audio loss
#                              # starting a few dozen segments in. Capping this well under
#                              # _MAX_AUDIO_RING leaves headroom so _PREFETCH_LEAD_SEGMENTS
#                              # - which already paces the background loop to roughly track
#                              # real playback position - can actually do its job.
_LATE_AUDIO_WAIT_FACTOR = 2.0  # _resolve_late_audio_window's retry budget, x seg_duration.
#                              # Was 6.0 (~30s on a 5s channel) on the theory that widening it
#                              # "costs nothing since it never blocks the critical path" - true,
#                              # but never actually calibrated against a real recovery time.
#                              # Checked against production logs after the windowed-mux rewrite:
#                              # every genuine recovery landed in well under 1s (the async retry's
#                              # 0.1s poll simply catching the audio prefetch loop's own ~1-poll-
#                              # interval race, not a real multi-second CDN delay) - the 30s
#                              # figure was only ever spent in full on segments with a genuine,
#                              # permanent content gap (no amount of waiting would have helped),
#                              # where it bought nothing but a slower "give up" conclusion. 2.0
#                              # (~10s) keeps comfortable margin over both the observed <1s real
#                              # recoveries and _fetch_one's own worst-case two-attempt retry
#                              # (up to ~2 x seg_duration x 0.8 with no gap between attempts)
#                              # without the pointless tail on a genuine gap.
_DISC_AUDIO_PEEK_WAIT = 1.5    # seconds _cut_and_mux_next_window's synchronous audio check
#                              # waits for a disc-introducing window specifically, instead of
#                              # the true zero-wait peek this used to be. Motivated directly by
#                              # production logs: every genuine recovery (audio that was simply
#                              # about to be appended, a scheduling race against the audio
#                              # prefetch loop's own ~1-poll-interval cadence, not a real delay)
#                              # landed in well under 1s. A short bounded wait here catches that
#                              # common case before ever publishing a silence-muxed window at
#                              # all - better than today's silence-then-maybe-upgrade dance - at
#                              # the cost of blocking the video prefetch loop for up to this long,
#                              # but only at real disc boundaries, not every segment. A genuine,
#                              # permanent content gap (no audio for this content at all, ever -
#                              # confirmed in production) isn't helped by any wait length here and
#                              # still falls through to the same silence + background retry path
#                              # as before, just 1.5s later than a true zero-wait would have.
_HANDLE_SEGMENT_DEADLINE_FACTOR = 1.0  # _handle_segment's provisional-wait deadline,
#                              # x seg_duration. Was 4.0 (~20s on a 5s channel) until a
#                              # 2026-07-17 incident: a content-transition segment whose
#                              # audio never matched held Enigma2's /seg GET open for the
#                              # full ~20s deadline before falling back to silence, and
#                              # gstreamer's own stall recovery caught up to the CDN's live
#                              # edge in that gap - the 4 segments in between were still
#                              # muxed and ready in _ready_segs (confirmed in the debug log)
#                              # but never requested, because they'd already scrolled out
#                              # of the CDN's live playlist window by the time Enigma2
#                              # asked for the next one. The wait here only exists to avoid
#                              # a narrower race (see its call site: a real mux landing
#                              # microseconds after a placeholder would've been served,
#                              # which Enigma2 then never re-requests) - the routine
#                              # discontinuities elsewhere in that same log all resolved in
#                              # under 1s, and the stuck one never resolved even given the
#                              # full 30s _LATE_AUDIO_WAIT_FACTOR budget, so a wider deadline
#                              # here wasn't actually buying real-audio outcomes, just
#                              # spending live-window real time. Capped to roughly one
#                              # segment's worth so a stuck segment can cost at most ~1
#                              # segment of the live window, not several.
_SLATE_WINDOW_BYTES = 200000   # a live channel whose first window is smaller than this (the 60-70 KB slate a new Pluto
#                              # stitch session starts with: 4.8 s of an almost static picture) is not started with
#                              # that window alone: _handle_playlist waits for a second one (_SLATE_WAIT). Given only
#                              # the slate, hlsdemux doesn't go on to preroll but asks for the playlist again after a
#                              # target duration (5 s); with the next window in the first playlist the start was
#                              # about 4 s sooner.
_SLATE_WAIT = 3.0              # seconds to wait for that second window
_COLD_START_FIRST_SEGMENT_WAIT = 5.0  # seconds _handle_playlist waits, on a channel's
#                              # very first playlist request only, for segment 0 to
#                              # actually be ready before replying at all. Observed in
#                              # production: GStreamer's hlsdemux gave up with an
#                              # "Invalid manifest" error (pipeline then permanently
#                              # stuck at READY, never recovering on its own) well
#                              # under 1s after the master fetch - tighter than the
#                              # time a cold first-segment audio fetch (fresh key
#                              # fetch + decrypt + CDN round-trip) can legitimately
#                              # take, even with the synchronous playlist response
#                              # itself now fast (see _AUDIO_PREPOPULATE_CAP). Holding
#                              # the *playlist* response until segment 0 is ready
#                              # moves the wait to a stage GStreamer already tolerates
#                              # (it happily waited ~0.5s for the master fetch in the
#                              # same trace) instead of gambling on its patience for
#                              # the *segment* fetch that follows. Bounded and best-
#                              # effort: if this expires, _handle_playlist falls back
#                              # to its previous behavior (reply immediately, let
#                              # _handle_segment's own synchronous wait bridge the
#                              # gap) exactly as before this existed - no regression
#                              # risk for a channel that's still legitimately slow
#                              # past this budget.
_VOD_COLD_START_MIN_SEGMENTS = 2  # VOD only (was 3; 1 froze shortly after the start - see the stall below): _handle_playlist's cold-start wait holds
#                              # out for at least this many ready segments (not just
#                              # the first one) before replying, up to
#                              # _VOD_COLD_START_WAIT. Motivated by a production
#                              # stall: with only segment 0 advertised, Enigma2 played
#                              # it, had nothing else queued, and sat idle for several
#                              # seconds until its own next playlist re-poll turned up
#                              # several more segments that had piled up ready in the
#                              # meantime - a real, audible stall right at the start of
#                              # every VOD title, immediately followed by a burst
#                              # request for all of them at once. Live keeps replying
#                              # on the first ready segment as before (this only
#                              # applies to a 'vod'-prefixed channel_id) - a live zap
#                              # should start the instant anything is available, and
#                              # live's own continuous real-time production rate means
#                              # there's nothing to batch up even if it waited.
#                              # 3 segments are 15 s of video, far more than the player
#                              # needs to keep going until its next playlist re-poll, and
#                              # waiting for 5 added about two more segments of
#                              # fetch+decrypt+mux (0.7 s each on a Dreambox) to every
#                              # VOD start.
_VOD_PRELOAD_SEGMENTS = 4       # VOD only: video segments _handle_playlist starts downloading at the first playlist
#                              # request (the video loop uses them), whatever the cold-start wait is
_VOD_AUDIO_PREPOPULATE_CAP = 4  # VOD only: the audio head start _handle_playlist fetches
#                              # synchronously before it replies. The video cold start
#                              # waits for _VOD_COLD_START_MIN_SEGMENTS segments, which
#                              # need that many audio segments plus one; the background
#                              # audio pre-fetch (paced by _PREFETCH_LEAD_SEGMENTS) brings
#                              # the rest in while the first segments are muxed. The
#                              # general _AUDIO_PREPOPULATE_CAP of 10 made the reply wait
#                              # for audio that was fetched twice (again by the background
#                              # pre-fetch) and not needed yet.
_VOD_COLD_START_WAIT = 10.0    # VOD-only cold-start budget, replacing
#                              # _COLD_START_FIRST_SEGMENT_WAIT's shorter one - waiting
#                              # for _VOD_COLD_START_MIN_SEGMENTS to actually be ready
#                              # (fetch+decrypt+mux each) legitimately takes longer than
#                              # the single-segment live case that budget was tuned for.
_TS_MASK = (1 << 33) - 1       # PTS/DTS/PCR-base are 33-bit fields with the same wraparound
_LIVE_GAP_TICKS = 3000         # start of a window after the end of the previous one (90kHz ticks) until measured
_MUX_ALIGN_MIN = 0.25          # seconds: _ffmpeg_mux shifts the audio to the start of the video window only for a
#                              # difference above this. The audio of a window starts at the first audio PES packet
#                              # at or after the start of the video window (_RenditionBuffer.cut), so it is up to one
#                              # packet late: 0.03-0.17 s for the audio description track of Hogan's Heroes, in every
#                              # window. Moving that audio back to the start of the window put it ahead of the
#                              # picture by that much, a different amount in every window (audible as audio and
#                              # video slightly out of sync). The audio is contiguous from window to window, so
#                              # with its own timestamps it is where it belongs.
_STALE_AUDIO_DELAY = 3.0       # seconds _slice_audio_for_window waits for the audio of a window's own epoch before it
#                              # falls back on the one of the epoch before. After a content transition the audio
#                              # rendition can be a segment behind the video one; the old epoch's buffer then still
#                              # holds the PREVIOUS clip at the same (restarted) timestamps, and a window of the new
#                              # clip used its audio at once (an ad's sound under a few seconds of a movie).
_STALE_COVER_SLACK = 45000     # 90 kHz ticks (0.5 s): audio of the epoch before only counts as covering a window if its
#                              # first and last packet are this close to the window's start and end. Such a buffer is
#                              # usually consumed up to its end already, and cut() then returns the few packets that
#                              # are left (3 packets for a 5 s window, shifted back by 5 s): a window without sound.
_AUDIO_JOIN_MAX_GAP = 45000    # 90 kHz ticks (0.5 s): the audio of a window continues where the previous one ended
#                              # if the video window starts at most this long after that point (_cut_and_mux_next_window)
_FFMPEG_TIMEOUT = 30           # seconds; _ffmpeg_mux is a stream-copy (no transcode),
#                              # so its runtime is dominated by fixed process-spawn +
#                              # temp-file I/O overhead, not segment playback duration
#                              # — a flat ceiling, not a seg_duration-scaled one. ~30x
#                              # the typically-observed sub-second mux time.
# All time budgets are derived per-channel from _ChannelState.seg_duration at runtime:
#   poll interval  = seg_duration / 5        (≈5× per segment)
#   fetch timeout  = seg_duration × 0.8
#   audio wait     = seg_duration × 1.6      (CDN lag spikes to ~1.4×, +~1 s margin)
#   audio wait @disc = _DISC_AUDIO_PEEK_WAIT (~1.5s; a short bounded peek, not a
#     true zero-wait - long enough to catch the common case (audio that was
#     simply about to be appended, a sub-second scheduling race - see that
#     constant's own comment) before ever publishing a silence-muxed window,
#     short enough that a disc-introducing segment still can't stall the video
#     prefetch loop for long. A genuine, permanent content gap isn't helped by
#     this wait either way and still falls through to silence + the background
#     retry below.)
#   late-audio retry = seg_duration x _LATE_AUDIO_WAIT_FACTOR (off to the side, see
#     _resolve_late_audio_window - a last chance for a genuinely slow (not
#     permanently missing) fetch to land in time for a re-request, off the
#     critical path)
#   handle_segment deadline = seg_duration x _HANDLE_SEGMENT_DEADLINE_FACTOR
#     (capped well below the late-audio budget above - see that constant's
#     comment - so a single stuck segment can't stall live delivery long
#     enough for the CDN's own window to scroll past not-yet-requested
#     segments before falling back to silence)
# There is no PTS-proximity tolerance anymore: _RenditionBuffer.cut() matches an exact
# [start_pts, end_pts) range via bisect on a continuous buffer, not a nearest-candidate
# search - either some buffer's index truly covers the range needed, or it doesn't.
_SESSION = requests.Session()

_FFMPEG = 'ffmpeg'           # system ffmpeg
_FFPROBE = 'ffprobe'         # system ffprobe; used once per channel to read the real
#                            # audio stream's sample rate/channel count (see _probe_audio_format)

# ── ffmpeg muxing helpers ─────────────────────────────────────────────────────


def _first_pts(data: bytes) -> 'int | None':
    """Return the first PTS (90 kHz ticks) found in raw MPEG-TS *data*.

    Scans up to the first 200 TS packets for a PES with a PTS field.
    Returns None if no PTS is found.
    """
    lim = min(len(data) - 187, 188 * 200)
    for off in range(0, lim, 188):
        if data[off] != 0x47:                            # sync byte
            continue
        if not ((data[off + 1] >> 6) & 1):              # PUSI must be set
            continue
        afc = (data[off + 3] >> 4) & 3
        if not (afc & 1):                               # must carry payload
            continue
        pl = off + 4 + (1 + data[off + 4] if afc & 2 else 0)  # payload start
        if pl + 14 > off + 188:
            continue
        if data[pl:pl + 3] != b'\x00\x00\x01':         # PES start code
            continue
        if not (data[pl + 7] & 0x80):                   # PTS_DTS_flags: PTS present
            continue
        b = data[pl + 9:pl + 14]
        return (((b[0] & 0x0E) << 29) | (b[1] << 22) |
                ((b[2] & 0xFE) << 14) | (b[3] << 7) |
                ((b[4] & 0xFE) >> 1))
    return None


def _probe_audio_format(path: str) -> 'tuple[int, int] | None':
    """Return (sample_rate, channels) of *path*'s first audio stream, or None on failure."""
    try:
        r = subprocess.run(
            [_FFPROBE, '-v', 'error', '-select_streams', 'a:0',
             '-show_entries', 'stream=sample_rate,channels',
             '-of', 'csv=p=0', path],
            capture_output=True, timeout=5, check=False,
        )
        if r.returncode != 0 or not r.stdout:
            return None
        sample_rate, channels = r.stdout.decode(errors='replace').strip().split(',')
        return int(sample_rate), int(channels)
    except Exception:
        return None


def _patch_continuity(data: bytes, start_cc: dict) -> 'tuple[bytes, dict]':
    """Rewrite each PID's continuity-counter nibble so it continues from
    *start_cc* instead of wherever ffmpeg's fresh mux process happened to
    start it (always 0 - a new process has no memory of the previous
    segment's last CC per PID).

    *start_cc* maps pid -> last CC used (or absent/-1 for "never seen",
    which reproduces a fresh 0-start - i.e. passing {} is a no-op measure
    pass that returns the same bytes ffmpeg produced). Returns
    (patched_bytes, ending_cc); the caller feeds ending_cc back in as
    start_cc for the next segment to keep the chain going.

    Only packets that actually carry a payload increment CC, per spec -
    adaptation-field-only packets (no payload) must not bump it.
    """
    packet_size = 188
    buf = bytearray(data)
    cc_state = dict(start_cc)
    n = len(buf) // packet_size
    for i in range(n):
        off = i * packet_size
        if buf[off] != 0x47:
            continue
        pid = ((buf[off + 1] & 0x1F) << 8) | buf[off + 2]
        afc = (buf[off + 3] >> 4) & 0x03
        if afc not in (0x01, 0x03):   # no payload -> CC must not change
            continue
        new_cc = (cc_state.get(pid, -1) + 1) % 16
        cc_state[pid] = new_cc
        buf[off + 3] = (buf[off + 3] & 0xF0) | new_cc
    return bytes(buf), cc_state


_WindowMeta = namedtuple('_WindowMeta', 'epoch start_pts end_pts is_disc')


def _scan_ts_index(ts_bytes: bytes) -> 'tuple[list[tuple[int, int]], bytes | None]':
    """Scan raw TS bytes for every PUSI'd PTS-carrying packet's (offset, pts)
    and the most recent PAT (pid 0) + immediately-following PMT packet pair.

    Generalizes _first_pts's single-packet early-exit scan to index every
    occurrence in the given bytes, not just the first - this is what lets
    _RenditionBuffer.cut() slice an exact PTS range instead of only ever
    handing back a whole segment. Pure function (no shared state touched),
    deliberately kept callable outside any lock: the per-packet scan is the
    expensive part of appending a segment to a _RenditionBuffer, and doing
    it while holding state._lock would stall every other thread's
    concurrent add/cut/trim call the same way logger.debug() would (see
    pop_audio_for_pts's own reasoning for logging outside the lock).
    """
    pts_offsets = []
    pat_pmt = None
    pending_pat = None
    n = len(ts_bytes) // 188
    for i in range(n):
        off = i * 188
        if ts_bytes[off] != 0x47:
            continue
        pid = ((ts_bytes[off + 1] & 0x1F) << 8) | ts_bytes[off + 2]
        if pid == 0:
            pending_pat = bytes(ts_bytes[off:off + 188])
            continue
        if pending_pat is not None and pid != 0x1FFF:
            # First non-PAT, non-null packet after a PAT is assumed to be
            # its PMT - true for every segment observed (a PAT is
            # immediately followed by its PMT, standard for a
            # single-program TS).
            pat_pmt = pending_pat + bytes(ts_bytes[off:off + 188])
            pending_pat = None
        if not ((ts_bytes[off + 1] >> 6) & 1):              # PUSI must be set
            continue
        afc = (ts_bytes[off + 3] >> 4) & 3
        if not (afc & 1):                                   # must carry payload
            continue
        pl = off + 4 + (1 + ts_bytes[off + 4] if afc & 2 else 0)  # payload start
        if pl + 14 > off + 188:
            continue
        if ts_bytes[pl:pl + 3] != b'\x00\x00\x01':          # PES start code
            continue
        if not (ts_bytes[pl + 7] & 0x80):                    # PTS_DTS_flags: PTS present
            continue
        b = ts_bytes[pl + 9:pl + 14]
        pts = (((b[0] & 0x0E) << 29) | (b[1] << 22) |
               ((b[2] & 0xFE) << 14) | (b[3] << 7) |
               ((b[4] & 0xFE) >> 1))
        pts_offsets.append((off, pts))
    return pts_offsets, pat_pmt


class _RenditionBuffer:
    """Continuous per-epoch byte buffer for one HLS rendition (video-only or
    audio-only TS).

    Bytes from consecutive CDN segments within the same epoch are appended
    in arrival order - real CDN PTS is already continuous within an epoch
    (the same reason -copyts is already correct for ordinary same-epoch
    segment pairs, see _ffmpeg_mux's docstring), so cut() can slice an
    exact PTS range out of this buffer freely, drawing across CDN segment
    boundaries instead of requiring one whole discrete segment to match
    another.

    A PTS index (pts_list/off_list, kept sorted by construction - see
    append()) is built incrementally alongside each append so cut() can
    binary-search for byte offsets instead of rescanning the whole buffer
    on every call.
    """
    __slots__ = ('epoch', 'is_disc_epoch', 'first_window_emitted',
                 'data', 'pts_list', 'off_list',
                 'last_pat_pmt', 'created_at', 'last_append_at', 'closed_at')

    def __init__(self, epoch: int, is_disc_epoch: bool):
        self.epoch = epoch
        self.is_disc_epoch = is_disc_epoch   # False only for an epoch-resync correction (see
        #                                    # _cut_and_mux_next_window) - a bookkeeping fix, not
        #                                    # a confirmed real transition. Every other caller,
        #                                    # including a freshly (re)started thread's very
        #                                    # first buffer, passes True - see that call site's
        #                                    # own comment for why a fresh buffer is disc-like
        #                                    # on every axis that matters here.
        self.first_window_emitted = False    # gates is_disc to exactly the first window cut from this buffer
        self.data = bytearray()
        self.pts_list: list = []
        self.off_list: list = []
        self.last_pat_pmt: 'bytes | None' = None
        self.created_at = time.monotonic()
        self.last_append_at = self.created_at
        self.closed_at: 'float | None' = None

    def append(self, ts_bytes: bytes, pts_offsets: list, pat_pmt: 'bytes | None'):
        """Extend the buffer with *ts_bytes*, whose PTS index and PAT/PMT
        were already scanned outside any lock by _scan_ts_index (offsets in
        *pts_offsets* are relative to the start of *ts_bytes*, rebased here
        to this buffer's own coordinates).

        Rejects the whole segment - bytes included, not appended at all -
        if it would go backwards relative to the buffer's current tail
        (e.g. two independent callers racing on overlapping seq ranges at
        channel startup; see _append_audio's own seq-level dedup for the
        primary guard against that). An earlier version of this method
        still appended ts_bytes unconditionally and only skipped the
        offending entries from the PTS index, defending pts_list/off_list's
        required monotonicity - but left that segment's raw bytes
        physically spliced into self.data anyway, unindexed and invisible
        to any cut() whose byte range doesn't happen to span them, but
        silently *included*, out of temporal order, in any cut() whose
        range does. Rejecting the whole segment here is the only way to
        keep every byte in self.data reachable exclusively through
        pts_list/off_list, which cut()'s slicing assumes.
        """
        if self.pts_list and pts_offsets and pts_offsets[0][1] < self.pts_list[-1]:
            return
        base = len(self.data)
        self.data += ts_bytes
        self.last_append_at = time.monotonic()
        for off, pts in pts_offsets:
            self.pts_list.append(pts)
            self.off_list.append(base + off)
        if pat_pmt is not None:
            self.last_pat_pmt = pat_pmt

    def cut(self, start_pts: int, end_pts: 'int | None'):
        """Return (sliced_bytes, actual_start_pts, actual_end_pts) covering
        the buffered index entries in [start_pts, end_pts), or None if not
        (yet) coverable. *end_pts*=None cuts everything currently buffered
        from start_pts on (used to flush a closing epoch's remainder).

        The returned bytes are prepended with the buffer's most recently
        observed PAT+PMT pair (see _scan_ts_index): a slice can start
        mid-CDN-segment, where the nearest PAT/PMT in the original stream
        may sit well before the slice's own start offset.
        """
        if not self.pts_list:
            return None
        start_i = bisect.bisect_left(self.pts_list, start_pts)
        if start_i >= len(self.pts_list):
            return None
        if end_pts is None:
            # Always hold back the buffer's current last indexed entry
            # rather than including it: the caller re-feeds this cut's own
            # actual_end straight back in as the *next* call's start_pts
            # (see _cut_and_mux_next_window), and bisect_left's inclusive
            # semantics would re-match that exact same entry as the start
            # of the next cut too - duplicating its bytes into both
            # windows. Holding it back here means the next cut's start_i
            # lands on this same not-yet-released entry, this time
            # correctly picking up bytes from where this cut actually
            # stopped (end_off below), not before it.
            end_i = len(self.pts_list) - 2
            if end_i < start_i:
                return None
        else:
            if self.pts_list[-1] < end_pts:
                return None
            end_i = bisect.bisect_left(self.pts_list, end_pts) - 1
            if end_i < start_i:
                return None
        start_off = self.off_list[start_i]
        end_off = self.off_list[end_i + 1] if end_i + 1 < len(self.off_list) else len(self.data)
        chunk = bytes(self.data[start_off:end_off])
        if self.last_pat_pmt is not None:
            chunk = self.last_pat_pmt + chunk
        return chunk, self.pts_list[start_i], self.pts_list[end_i]

    def trim_before(self, pts: int, retain_bytes: int = 0):
        """Drop the consumed prefix (bounding memory), keeping *retain_bytes*
        behind the trim point so the narrowed on-demand fallback in
        _handle_segment can still re-derive a just-produced window without
        a fresh CDN re-fetch (see _VIDEO_BUFFER_RETAIN_MARGIN_BYTES).
        """
        i = bisect.bisect_left(self.pts_list, pts)
        if i <= 0:
            return
        cut_off = max(0, self.off_list[i] - retain_bytes)
        if cut_off <= 0:
            return
        del self.data[:cut_off]
        self.off_list = [o - cut_off for o in self.off_list]
        keep = bisect.bisect_left(self.off_list, 0)
        if keep > 0:
            self.pts_list = self.pts_list[keep:]
            self.off_list = self.off_list[keep:]

    def trim_to_max_bytes(self, max_bytes: int):
        """Drop the oldest indexed entries (and their bytes) until this
        buffer's total size is at most *max_bytes*, regardless of whether
        anything has actually consumed them via a successful cut() yet.

        See _MAX_AUDIO_BUFFER_BYTES - a defensive backstop for a buffer
        that never gets a successful match at all (trim_before() alone
        never fires for one), not a routine eviction path.
        """
        if len(self.data) <= max_bytes or not self.off_list:
            return
        target_off = len(self.data) - max_bytes
        i = bisect.bisect_left(self.off_list, target_off)
        if i <= 0 or i >= len(self.off_list):
            return
        cut_off = self.off_list[i]
        del self.data[:cut_off]
        self.off_list = [o - cut_off for o in self.off_list[i:]]
        self.pts_list = self.pts_list[i:]

    def mark_closed(self):
        """Stop receiving appends but stay readable (e.g. for
        _slice_audio_for_window's one-epoch-back fallback) until pruned -
        see _prune_audio_bufs.
        """
        self.closed_at = time.monotonic()


def _ffmpeg_mux(video_data: bytes, audio_data: 'bytes | None' = None,
                state: '_ChannelState | None' = None, is_disc: bool = False,
                track_cc: bool = False, reset_cc: 'bool | None' = None,
                v_pts: 'int | None' = None, a_pts: 'int | None' = None) -> bytes:
    """Mux video-only and audio-only TS segments via system ffmpeg.

    Both inputs are written to seekable temp files so ffmpeg can probe each
    stream's start_time and normalise output PTS.  When *audio_data* is None
    the single stream is re-muxed without explicit stream mapping.
    Falls back to returning the input data unchanged on any ffmpeg error.

    itsoffset is derived from the PTS difference between the two streams -
    it reflects whatever CDN-side alignment exists between them, typically
    near-zero for an ordinary window and up to the PTS span of one window
    at most for a genuine content transition.

    *v_pts*/*a_pts*, if given, are used directly instead of independently
    re-deriving them with _first_pts. Every current caller already knows
    both exactly - they came straight from _RenditionBuffer.cut()'s return
    when the slices being muxed here were cut - so scanning the same bytes
    a second time here would be pure waste. Left optional (falling back to
    _first_pts) so this function stays independently correct if ever called
    with bytes whose PTS isn't already known by the caller.

    When *state* is given, the real audio segment's sample rate/channel count
    is probed once and cached on state.audio_format so later silence-injected
    segments (see the *audio_data* is None branch below) match it instead of
    a hardcoded guess - switching the decoder between two different audio
    formats mid-stream is its own, separate source of playback trouble on top
    of whatever a real content transition already causes.

    -copyts preserves each stream's real absolute PTS/PCR instead of ffmpeg's
    default of rebasing every independent mux to start near zero - confirmed
    against real segment pairs that the underlying CDN timeline is already
    continuous across ordinary segment boundaries, so this alone is enough
    to stop PCR/PTS from resetting every ~5s. It does *not* touch continuity
    counters, which every fresh ffmpeg process still starts at 0 per PID
    regardless - that part is fixed separately below via _patch_continuity.

    *is_disc* controls whether this segment is flagged as a genuine content
    transition (-mpegts_flags initial_discontinuity, matching a real CDN
    #EXT-X-DISCONTINUITY - the underlying encoder actually restarted, so a
    real PCR/PTS jump here is correct, not a bug) versus an ordinary segment
    that should carry on the previous one's timeline with no flag at all.
    This is the one flag consumers (GStreamer's hlsdemux for live, and
    RecordingProxyUnmuxed's _segment_is_disc/_continuize for recordings) ever see
    baked into the TS bytes - it must stay tied to a *real* transition only.

    *track_cc* opts this call into the shared per-channel CC continuation
    chain in state._cc_state, updating it for the next call. Only safe for
    the primary, strictly-in-order video-prefetch path: state._cc_state
    reflects "whatever the highest seq processed so far ended at", so a
    call that might land out of order relative to that (the late-audio
    upgrade overwriting an already-passed seq, or the disconnected
    synchronous fallback) must leave it False - they still get -copyts and
    an is_disc flag that accurately reflects whether their segment is a
    real transition, just not chained CC continuity.

    *reset_cc* controls only whether _patch_continuity restarts the CC chain
    at {} instead of continuing state._cc_state - defaults to *is_disc* (a
    real transition always needs a fresh CC baseline too) but can be set
    True independently of is_disc, for a segment whose *content* is
    continuous but whose CC baseline needs restarting for an unrelated
    bookkeeping reason (see state._cc_resync_pending). Deliberately a
    separate parameter from is_disc: forcing is_disc True to get a CC reset
    would also plant a false -mpegts_flags initial_discontinuity in that
    segment's bytes, telling GStreamer's hlsdemux (or RecordingProxyUnmuxed) a real
    content cut happened right there when it didn't.
    """
    if reset_cc is None:
        reset_cc = is_disc
    vfile = None
    afile = None
    try:
        with tempfile.NamedTemporaryFile(
                prefix='stproxy_v_', suffix='.ts', dir='/tmp', delete=False) as tf:
            tf.write(video_data)
            vfile = tf.name
        if audio_data:
            with tempfile.NamedTemporaryFile(
                    prefix='stproxy_a_', suffix='.ts', dir='/tmp', delete=False) as tf:
                tf.write(audio_data)
                afile = tf.name
    except OSError as exc:
        logger.debug('temp file write failed: %s', exc)
        for p in filter(None, (vfile, afile)):
            try:
                os.unlink(p)
            except OSError:
                pass
        return video_data

    if v_pts is None:
        v_pts = _first_pts(video_data)
    itsoffset = 0.0
    if audio_data:
        if a_pts is None:
            a_pts = _first_pts(audio_data)
        if v_pts is not None and a_pts is not None:
            itsoffset = (v_pts - a_pts) / 90000.0
            if abs(itsoffset) > 0.020:
                logger.debug('_ffmpeg_mux: pts v=%s a=%s itsoffset=%.3fs%s',
                             v_pts, a_pts, itsoffset, ' (not applied)' if abs(itsoffset) <= _MUX_ALIGN_MIN else '')
            if abs(itsoffset) <= _MUX_ALIGN_MIN:
                itsoffset = 0.0   # the audio packet boundary, not a misalignment of the two renditions
        else:
            logger.debug('_ffmpeg_mux: _first_pts failed v=%s a=%s', v_pts, a_pts)
        if state is not None and state.audio_format is None:
            fmt = _probe_audio_format(afile)
            if fmt is not None:
                state.audio_format = fmt

    mpegts_flags = 'initial_discontinuity' if is_disc else ''

    # -pes_payload_size 0: one PES packet (with its own PTS) per audio frame, as Pluto's own segments have. The muxer's
    # default packs about 11 frames (235 ms) into one PES packet with a single PTS; the Dreambox's audio decoder then
    # played the audio at a different offset from the picture than it does for the original stream (direct playback
    # without the proxy is in sync).
    if audio_data:
        cmd = [_FFMPEG, '-y', '-loglevel', 'error',
               '-probesize', '200000', '-analyzeduration', '1000000',
               '-i', vfile,
               '-probesize', '200000', '-analyzeduration', '1000000',
               '-itsoffset', f'{itsoffset:.6f}',
               '-i', afile,
               '-c', 'copy',
               '-map', '0:v:0', '-map', '1:a:0',
               '-f', 'mpegts', '-copyts', '-pes_payload_size', '0']
        if mpegts_flags:
            cmd += ['-mpegts_flags', mpegts_flags]
        cmd += ['-pat_period', '2', 'pipe:1']
    elif v_pts is not None:
        # No real audio available; inject silence at the video's PTS so the
        # output TS carries an audio track.  Enigma2 permanently drops audio
        # when it receives even one video-only segment; a silent audio track
        # keeps the decoder alive until real audio returns in the next segment.
        # -itsoffset shifts the null source to start at the same PTS as the
        # video; -shortest stops encoding when the video ends (~5 s).
        # Match the real audio's sample rate/channel count when known (see
        # state.audio_format above) instead of a hardcoded guess: switching
        # the hardware decoder between two audio formats mid-stream is its
        # own source of resync trouble, separate from the PTS reset itself.
        if state is not None and state.audio_format is not None:
            _sample_rate, _channels = state.audio_format
        else:
            _sample_rate, _channels = 48000, 2
        # anullsrc takes channel_layout (a name or channel count), not a
        # 'channels' option - that's an invalid option name that fails the
        # whole mux, which is worse than the mismatch this was meant to fix:
        # it falls through to returning video-only data, which is what
        # permanently drops audio in Enigma2 in the first place.
        _channel_layout = {1: 'mono', 2: 'stereo'}.get(_channels, 'stereo')
        cmd = [_FFMPEG, '-y', '-loglevel', 'error',
               '-probesize', '200000', '-analyzeduration', '1000000',
               '-i', vfile,
               '-itsoffset', f'{v_pts / 90000.0:.6f}',
               '-f', 'lavfi', '-i', f'anullsrc=channel_layout={_channel_layout}:sample_rate={_sample_rate}',
               '-c:v', 'copy', '-c:a', 'aac', '-b:a', '64k',
               '-map', '0:v:0', '-map', '1:a:0',
               '-shortest',
               '-f', 'mpegts', '-copyts', '-pes_payload_size', '0']
        if mpegts_flags:
            cmd += ['-mpegts_flags', mpegts_flags]
        cmd += ['-pat_period', '2', 'pipe:1']
    else:
        # PTS unreadable; plain single-stream re-mux as last resort.
        cmd = [_FFMPEG, '-y', '-loglevel', 'error',
               '-probesize', '200000', '-analyzeduration', '1000000',
               '-i', vfile,
               '-c', 'copy', '-f', 'mpegts', '-copyts', '-pes_payload_size', '0']
        if mpegts_flags:
            cmd += ['-mpegts_flags', mpegts_flags]
        cmd += ['-pat_period', '2', 'pipe:1']

    try:
        r = subprocess.run(cmd, capture_output=True, timeout=_FFMPEG_TIMEOUT, check=False)
        if r.returncode == 0 and r.stdout:
            if track_cc and state is not None:
                start_cc = {} if reset_cc else state._cc_state
                patched, state._cc_state = _patch_continuity(r.stdout, start_cc)
                return patched
            return r.stdout
        label = 'av' if audio_data else ('silence' if v_pts is not None else 'video-only')
        logger.debug('_ffmpeg_mux %s rc=%s out=%sb: %s',
                     label, r.returncode, len(r.stdout), r.stderr.decode(errors="replace")[-300:])
        return video_data
    except Exception as exc:
        logger.debug('_ffmpeg_mux exception: %s', exc)
        return video_data
    finally:
        for p in filter(None, (vfile, afile)):
            try:
                os.unlink(p)
            except OSError:
                pass

# ── Per-channel proxy state ──────────────────────────────────────────────────


class _ChannelState:
    """Holds state accumulated while proxying one live channel."""

    __slots__ = ('master_url', 'url_refresher', '_headers', 'refresh_on_failure', 'session_param', '_master_lock',
                 'audio_url', 'variant_urls',
                 'seg_duration', 'last_access', 'audio_format',
                 '_video_buf', '_video_cursor_pts', '_audio_bufs',
                 '_audio_epoch', '_audio_disc_seqs_seen', '_audio_appended_seqs', '_video_epoch',
                 '_next_chunk_seq', '_window_meta',
                 '_last_requested_seq', '_ready_segs',
                 '_audio_prepopulated', '_provisional_segs',
                 '_audio_prefetch_thread', '_audio_prefetch_stop',
                 '_video_prefetch_thread', '_video_prefetch_stop', '_lock', '_cc_state',
                 '_cc_resync_pending', '_last_playlist_out', '_last_segment_out',
                 '_pending_audio_floors', '_vod_complete', '_video_preload', '_video_first_playlist', '_audio_slice_end',
                 '_live_delta', '_live_expected', '_live_prev_end', '_live_gap',
                 '_window_delta', '_shift_cache')

    @property
    def headers(self) -> 'dict | None':
        headers = self._headers
        return headers() if callable(headers) else headers

    @headers.setter
    def headers(self, headers):
        self._headers = headers

    def __init__(self, master_url: str):
        self.master_url = master_url     # '' while the provider has not resolved it yet, see resolved_master_url()
        self.url_refresher = None        # callable() -> fresh master URL; called from a worker thread (it may block)
        self._headers = None             # extra HTTP headers every request of this channel carries: a dict, a callable
        #                                # returning one (evaluated on the worker that needs it, so it may block), or None
        self.session_param = 'sid'       # query parameter of the master URL that carries the id of the stream session
        self.refresh_on_failure = False  # call url_refresher when the master playlist can't be fetched, not just
        #                                # for an empty JWT
        self._master_lock = threading.Lock()
        self.seg_duration: float = _SEG_DURATION  # updated from #EXTINF on first playlist parse
        self.last_access = time.monotonic()  # bumped on every HTTP request; drives the idle reaper
        self.audio_url = None            # resolved absolute audio rendition playlist URL
        self.variant_urls = []           # resolved absolute video variant playlist URLs
        self.audio_format: 'tuple[int, int] | None' = None  # (sample_rate, channels) probed from the
        #                                 # first real audio segment; used so anullsrc-injected silence
        #                                 # matches the real stream instead of a hardcoded guess
        self._video_buf: '_RenditionBuffer | None' = None  # continuous current-epoch video buffer;
        #                                 # created by _video_prefetch_loop on (re)start
        self._video_cursor_pts: 'int | None' = None  # start_pts of the next window to cut
        #                                 # from _video_buf; None until the buffer's first append
        self._audio_bufs: 'list[_RenditionBuffer]' = []  # newest-epoch-first; current epoch's
        #                                 # buffer plus up to _AUDIO_BUFFER_RETAIN_EPOCHS closed
        #                                 # ones retained for _slice_audio_for_window's
        #                                 # one-epoch-back fallback - see _prune_audio_bufs
        self._audio_epoch = 0            # bumped on each audio-rendition #EXT-X-DISCONTINUITY
        self._audio_disc_seqs_seen: 'set[int]' = set()  # seq -> already resolved a disc bump;
        #                                 # narrow _SEQ_REPROCESS_WINDOW dedup, see _append_audio
        self._audio_appended_seqs: 'set[int]' = set()  # seq -> already appended to a buffer;
        #                                 # guards the one-shot pre-population pass and the
        #                                 # background audio pre-fetch loop both landing the
        #                                 # same early seq twice (see _append_audio)
        self._video_epoch = 0            # mirrors _video_prefetch_loop's local video_epoch
        self._next_chunk_seq = 0         # monotonic; minted once per produced output window,
        #                                 # never reused - the _ready_segs key and /seg URL
        #                                 # numbering, decoupled entirely from CDN media-sequence
        #                                 # numbers (which differ between video and audio anyway)
        self._window_meta: 'OrderedDict[int, _WindowMeta]' = OrderedDict()  # chunk_seq ->
        #                                 # (epoch, start_pts, end_pts, is_disc) for each entry
        #                                 # currently in _ready_segs; pruned in lockstep with it
        self._last_requested_seq = 0     # floor for _PREFETCH_LEAD_SEGMENTS pacing; bumped by
        #                                 # real segment requests (_handle_segment, see
        #                                 # record_segment_request) reflecting actual playback
        #                                 # progress (pauses, seeks, variable buffering), and
        #                                 # also seeded from each playlist poll's own reported
        #                                 # position (both pre-fetch loops) so a fresh live zap
        #                                 # - whose real media-sequence numbers can already be
        #                                 # in the thousands - never starts out throttled
        #                                 # against this bare 0 default.
        self._audio_prepopulated = False  # set after the one-shot pre-population pass runs once
        self._audio_prefetch_thread: 'threading.Thread | None' = None
        self._audio_prefetch_stop = threading.Event()
        self._ready_segs: 'OrderedDict[int, bytes]' = OrderedDict()  # chunk_seq -> muxed bytes
        #                                 # (produced by _cut_and_mux_next_window)
        self._provisional_segs: set = set()  # chunk_seqs in _ready_segs muxed with silence,
        #                                 # pending a _resolve_late_audio_window upgrade; see mark_provisional
        self._video_prefetch_thread: 'threading.Thread | None' = None
        self._video_prefetch_stop = threading.Event()
        self._lock = threading.Lock()
        self._cc_state: dict = {}        # pid -> last continuity-counter used, chaining
        #                                 # _ffmpeg_mux's per-segment output across segments
        #                                 # (see _patch_continuity); written only by the video
        #                                 # prefetch thread, in seq order, so unlike most other
        #                                 # fields here it needs no lock of its own.
        self._cc_resync_pending = False  # set by _resolve_late_audio when it overwrites an
        #                                 # already-chained segment with different content
        #                                 # (silence -> real audio) after the fact; consumed by
        #                                 # the video prefetch loop to force its next segment's
        #                                 # reset_cc (not is_disc - see _ffmpeg_mux) so it
        #                                 # restarts the CC chain instead of continuing from a
        #                                 # baseline that no longer matches what was actually
        #                                 # served for the segment before it, without also
        #                                 # planting a false content-transition flag in a
        #                                 # segment that isn't actually one.
        self._last_playlist_out: 'dict[int, tuple[bytes, float]]' = {}  # variant idx -> (last
        #                                 # successfully rewritten playlist body, monotonic time
        #                                 # cached) - served instead of a 502 when a transient CDN
        #                                 # read-timeout/error hits the real variant playlist fetch
        #                                 # in _handle_playlist (see its use there). A single such
        #                                 # blip surfacing as an HTTP error straight to Enigma2 on
        #                                 # what looks like a routine manifest refresh is a
        #                                 # suspected cause of GStreamer's hlsdemux wedging the
        #                                 # whole pipeline - observed in production as playback
        #                                 # going black with no further segment requests, while
        #                                 # this proxy's own background pre-fetch threads (which
        #                                 # just retry next poll on the same kind of failure) kept
        #                                 # running fine for hours afterward. Re-serving the last
        #                                 # good body is stale by at most one poll interval
        #                                 # (~seg_duration), which is invisible in practice.
        self._last_segment_out: 'tuple[int, bytes] | None' = None  # (seq, muxed bytes) last
        #                                 # actually sent to Enigma2 by _handle_segment, from any
        #                                 # of its three send points - same reasoning as
        #                                 # _last_playlist_out, applied to the on-demand fallback's
        #                                 # own two hard-error paths (no cached segment info at
        #                                 # all, or the on-demand CDN fetch itself failing): a
        #                                 # transient blip surfacing as a 404/502 on a segment
        #                                 # Enigma2 is actively trying to consume is at least as
        #                                 # likely to wedge hlsdemux as the playlist case above,
        #                                 # with no request even reaching this handler again
        #                                 # afterward to recover on its own. Repeating one stale
        #                                 # segment is a brief visible stutter, not a silent
        #                                 # staleness the way a playlist is - but that's still far
        #                                 # better than the pipeline dying outright with no
        #                                 # recourse but a manual re-zap.
        # Timeline continuity for what is served to Enigma2, see assign_live_delta/timeline_adjusted.
        self._live_delta = 0             # shift (90kHz ticks, signed) the windows published from now on get
        self._live_expected = None       # where the next window should start, in shifted time
        self._live_prev_end = None       # end PTS of the previous window as muxed (unshifted)
        self._live_gap = _LIVE_GAP_TICKS  # measured distance between one window's end and the next one's start
        self._window_delta: dict = {}    # chunk_seq -> the shift of that window
        self._shift_cache: dict = {}     # chunk_seq -> (muxed bytes, shifted bytes) of the latest served
        self._video_preload: 'dict[str, Future]' = {}   # VOD start only: video segment url -> its download, started
        #                                 # by _handle_playlist while the audio is still being fetched
        self._audio_slice_end: 'tuple[int, int] | None' = None   # live: (epoch, end pts) of the audio range the previous
        #                                 # window was given, where the next window's audio range continues
        self._video_first_playlist: 'tuple[str, str] | None' = None   # VOD start only: (url, text) of the video
        #                                 # playlist _handle_playlist just fetched, for the video loop's first poll
        self._vod_complete = False       # VOD only: every segment of the asset has been fetched
        #                                 # and muxed (set by _video_prefetch_loop); lets
        #                                 # _handle_playlist finally advertise #EXT-X-ENDLIST
        self._pending_audio_floors: 'list[tuple[int, int]]' = []  # (epoch, start_pts) for
        #                                 # every _resolve_late_audio_window retry currently in
        #                                 # flight - see add_pending_audio_floor/audio_trim_floor.
        #                                 # Registered synchronously by _cut_and_mux_next_window
        #                                 # (not inside the spawned retry thread itself - that
        #                                 # would leave a gap between "window published as
        #                                 # uncovered" and "floor actually registered" for a
        #                                 # concurrent trim to slip through) the moment a window
        #                                 # is published uncovered, removed by the retry's own
        #                                 # finally block when it concludes either way.

    def current_audio_epoch(self) -> int:
        """Thread-safe read of the current epoch, with no side effects."""
        with self._lock:
            return self._audio_epoch

    def try_start_audio_prepopulate(self) -> bool:
        """Atomically claim the one-shot audio pre-population pass.

        Returns True for the first caller only; every later call (including
        concurrent ones racing on the first request) returns False. Must be
        a real flag rather than inferring "not yet run" from buffer state
        (e.g. state._audio_bufs being empty): a fresh channel legitimately
        has no audio buffer yet for reasons unrelated to whether
        pre-population has run, and re-running this block would re-parse the
        same still-in-window #EXT-X-DISCONTINUITY tag the live audio
        pre-fetch thread already counted, double-bumping _audio_epoch for
        one real transition (see _append_audio's own dedup, which this flag
        complements at a coarser, whole-pass granularity).
        """
        with self._lock:
            if self._audio_prepopulated:
                return False
            self._audio_prepopulated = True
            return True

    def set_video_epoch(self, epoch: int):
        """Record _video_prefetch_loop's current epoch on shared state, so a
        respawned thread instance can resume from it - see
        current_video_epoch()'s own docstring. Not read by anything else:
        buffer selection/pruning today is keyed by _RenditionBuffer.epoch
        directly (see _slice_audio_for_window/_prune_audio_bufs), not by
        comparing against this mirrored value.
        """
        with self._lock:
            self._video_epoch = epoch

    def current_video_epoch(self) -> int:
        """Thread-safe read of the last-recorded video-side epoch.

        _video_prefetch_loop seeds its local video_epoch counter from this
        instead of hardcoding 0, so that a restarted thread instance (e.g.
        _handle_playlist respawning it after an uncaught exception killed
        the previous one) resumes at the epoch already recorded on this
        shared state rather than racing back from scratch. _audio_epoch
        doesn't need this because it's mutated directly on self, immune to
        the audio thread restarting; video_epoch used to be a bare local
        variable with no such persistence, which let a respawned thread's
        video_epoch=0 sit desynced from an already-advanced _audio_epoch for
        several segments until the resync-on-confirmation logic caught up
        (observed as a real, if self-healing, multi-segment silence gap).
        """
        with self._lock:
            return self._video_epoch

    def record_segment_request(self, seq: int):
        """Record that Enigma2 has actually requested segment *seq*.

        Called from _handle_segment for every request, hit or miss. Read by
        both pre-fetch threads to pace themselves against real playback
        progress - see _PREFETCH_LEAD_SEGMENTS.
        """
        with self._lock:
            self._last_requested_seq = max(self._last_requested_seq, seq)

    def last_requested_seq(self) -> int:
        """Thread-safe read of the highest segment actually requested so far."""
        with self._lock:
            return self._last_requested_seq

    def has_ready_segs(self) -> bool:
        """True once the video prefetch loop has produced anything at all.

        Lets _handle_playlist distinguish "prefetch hasn't caught up to
        this one specific new segment yet" (worth a short wait - see
        is_ready's use) from "this channel just registered and nothing has
        been fetched at all yet" (must not wait, or the very first playlist
        response would come back empty and stall live-start).
        """
        with self._lock:
            return bool(self._ready_segs)

    def first_ready_size(self) -> int:
        """Size in bytes of the oldest produced segment, 0 if there is none."""
        with self._lock:
            return len(next(iter(self._ready_segs.values()))) if self._ready_segs else 0

    def ready_seg_count(self) -> int:
        """Thread-safe count of currently-produced segments.

        Used by _handle_playlist's VOD cold-start wait to hold out for
        several ready segments (see _VOD_COLD_START_MIN_SEGMENTS), not just
        the first one has_ready_segs() alone would confirm.
        """
        with self._lock:
            return len(self._ready_segs)

    def assign_live_delta(self, seq: int, start_pts: int, end_pts: int, is_disc: bool,
                          approximate: bool = False):
        """Decide how far window *seq* is shifted on its way to Enigma2 so the timeline never jumps.

        At a genuine content transition (is_disc: an ad starts or ends) the CDN timeline jumps, e.g. from the
        movie's PTS around 8.4 billion (the 33-bit counter wraps at 8.59 billion) to an ad starting at 90000,
        which a 33-bit decoder sees as a jump of 30 minutes. The decoder of the dm9xx shows distorted video
        for a few seconds after such a jump while audio plays on. Every window published from the transition on
        therefore carries a shift that makes it start where the previous one ended; timeline_adjusted() applies
        it when the window is served. state._ready_segs keeps the real timestamps: RecordingProxyUnmuxed does the same
        for recordings from them (its shift finds nothing left to do for the live part).

        Must be called with self._lock held, once per published window, in publishing order.
        *approximate* is for a window whose PTS range is only guessed (see the standalone fallback of
        _video_prefetch_loop): it gets the shift in effect and doesn't move the expectation.
        """
        if not approximate:
            if is_disc and self._live_expected is not None:
                delta = (self._live_expected - start_pts) & _TS_MASK
                self._live_delta = delta - (1 << 33) if delta >= 1 << 32 else delta   # signed, 33-bit
            elif self._live_prev_end is not None:
                gap = (start_pts - self._live_prev_end) & _TS_MASK
                if 0 < gap < 90000:
                    self._live_gap = gap
            self._live_prev_end = end_pts
            self._live_expected = (end_pts + self._live_delta + self._live_gap) & _TS_MASK
        self._window_delta[seq] = self._live_delta
        while len(self._window_delta) > _MAX_SEG_CACHE:
            self._window_delta.pop(next(iter(self._window_delta)))

    def reset_live_delta(self):
        """Forget the timeline (the cached windows are dropped, see register_channel). Lock held by the caller."""
        self._live_delta = 0
        self._live_expected = None
        self._live_prev_end = None
        self._live_gap = _LIVE_GAP_TICKS
        self._window_delta.clear()
        self._shift_cache.clear()

    def timeline_adjusted(self, seq: int, data: bytes) -> bytes:
        """*data* (window *seq* as muxed) with its timestamps shifted as assign_live_delta decided."""
        with self._lock:
            delta = self._window_delta.get(seq, 0)
            cached = self._shift_cache.get(seq)
        if not delta:
            return data
        if cached is not None and cached[0] is data:
            return cached[1]
        from .RecordingProxyUnmuxed import _rewrite_timeline  # deferred: RecordingProxyUnmuxed imports from this module
        out = _rewrite_timeline(data, delta)
        with self._lock:
            self._shift_cache[seq] = (data, out)
            while len(self._shift_cache) > 6:
                self._shift_cache.pop(next(iter(self._shift_cache)))
        return out

    def mark_provisional(self, seq: int):
        """Record that _ready_segs[seq] is a silence-muxed placeholder with a
        _resolve_late_audio_window upgrade in flight, not the final AV mux.

        _handle_segment consults this so its existing wait loop actually
        waits for the upgrade instead of returning the placeholder the
        instant it sees any bytes at all in _ready_segs[seq].

        No self-pruning here (unlike the seq-keyed dedup sets elsewhere in
        this file): *seq* is chunk_seq, this proxy's own monotonic counter,
        which - unlike a CDN media-sequence number - never wraps or repeats,
        so there's no stale-reuse hazard to guard against by age. Entries
        are removed only by the matching clear_provisional() call, which
        _resolve_late_audio_window always makes exactly once, in its
        finally block, regardless of outcome - pruning by age here as well
        would risk dropping a still-genuinely-provisional entry early
        (e.g. during a burst of several transitions close together) and
        letting _handle_segment treat an unresolved upgrade as finished.
        """
        with self._lock:
            self._provisional_segs.add(seq)

    def clear_provisional(self, seq: int):
        """Mark seq's _resolve_late_audio_window retry concluded (upgraded or not)."""
        with self._lock:
            self._provisional_segs.discard(seq)

    def add_pending_audio_floor(self, epoch: int, start_pts: int):
        """Register that a _resolve_late_audio_window retry is still waiting
        to cover [start_pts, ...) of *epoch*'s audio buffer.

        See audio_trim_floor - a later window's own successful audio match
        on the same buffer must not trim away bytes this retry still needs,
        even though the trim is otherwise perfectly safe from the later
        window's own point of view.
        """
        with self._lock:
            self._pending_audio_floors.append((epoch, start_pts))

    def remove_pending_audio_floor(self, epoch: int, start_pts: int):
        """Undo add_pending_audio_floor once a retry concludes, either way."""
        with self._lock:
            try:
                self._pending_audio_floors.remove((epoch, start_pts))
            except ValueError:
                pass

    def audio_trim_floor(self, epoch: int, requested_pts: int) -> int:
        """Clamp a trim point to never go past any still-in-flight
        _resolve_late_audio_window retry's own start_pts for *epoch*.

        Without this, _slice_audio_for_window's trim_before() call - made
        on every successful match, including one found by a *later* window
        while an *earlier* window's background retry is still polling the
        same shared buffer - can delete the earlier window's still-needed
        range out from under it. The lock inside trim_before/append
        prevents the two calls from corrupting the buffer concurrently, but
        does nothing to stop this logical data loss: the earlier retry then
        exhausts its whole wait budget and reports "no audio ever covered
        this window" even though matching audio genuinely arrived - it was
        just consumed by the later window's own trim first.

        Caller must already hold self._lock: its one call site
        (_slice_audio_for_window) reads/trims the same buffer under that
        same lock, and self._lock is a plain Lock, not an RLock - a second,
        nested acquisition here would deadlock the thread against itself,
        not just race another one.
        """
        pending = [p for e, p in self._pending_audio_floors if e == epoch]
        return min([requested_pts] + pending) if pending else requested_pts

    def discard_stale_stream(self):
        """Drop everything served/buffered from before the pre-fetch threads stopped (or from a
        previous stream session), so a restarted pipeline does not glue fresh data onto stale
        data. Shared by register_channel and reopen()."""
        with self._lock:
            self._vod_complete = False
            self._ready_segs.clear()
            self._window_meta.clear()
            self._provisional_segs.clear()
            self.reset_live_delta()
            # Same reasoning, applied to the audio side: on
            # was_closed, the prefetch threads actually died, so
            # state._audio_bufs still holds whatever audio arrived
            # *before* the stop - real-time-continuous, so a fresh
            # append once fetching resumes would otherwise glue new
            # audio straight onto the stale tail with a silent,
            # untracked gap in between (append()'s out-of-order
            # guard only rejects PTS moving *backwards*). Cleared
            # here - synchronously, before this call returns the
            # proxy URL Enigma2 will use to make its first request -
            # rather than at the top of _audio_prefetch_loop itself:
            # that thread's start() is only a schedule request, and
            # _handle_playlist's synchronous pre-population can (and
            # did, in production) already append real fresh data
            # before the new thread's first line gets CPU time,
            # which a reset there would then race and wipe out.
            self._audio_bufs.clear()
            # _append_audio dedups by CDN seq via these two sets so
            # the sync pre-population pass and the background
            # prefetch thread (which independently parse the same
            # playlist) don't double-append the same seq. A
            # live playlist has a sliding window of ~15-30s, so a
            # short interruption (e.g. briefly watching a recording)
            # can still show the *same* low seqs this channel already
            # fetched and recorded here before the stop. Left
            # uncleared, _audio_bufs.clear() above empties the buffer
            # but _append_audio then silently no-ops on every one of
            # those seqs as "already appended" - pre-population
            # reports fetching N segments successfully while the
            # buffer it should have populated stays permanently
            # empty. Confirmed in production as the "no buffers
            # held" / stream-doesn't-restart bug on returning to the
            # same live channel_id after a recording.
            self._audio_appended_seqs.clear()
            self._audio_disc_seqs_seen.clear()

    def reopen(self) -> bool:
        """Undo close() so the pre-fetch threads can run again. Returns True if the channel had been closed.

        The stop Events are one-shot: left set, every thread started for this state exits at once. A
        recording that reconnects to /rec/ after the live slot closed the channel (register_channel is
        never called on that path) must revive the pipeline itself, see _ensure_pipeline.
        """
        was_closed = self._audio_prefetch_stop.is_set()
        if was_closed:
            self.discard_stale_stream()
        self._audio_prefetch_stop.clear()
        self._video_prefetch_stop.clear()
        if was_closed:
            self._audio_prepopulated = False
        return was_closed

    def evict_played(self, keep_behind: int):
        """VOD only: drop the segments more than *keep_behind* behind the last one Enigma2 requested.

        Called from _handle_segment after the request is recorded. Live channels are rings and never need this."""
        with self._lock:
            floor = self._last_requested_seq - keep_behind
            while self._ready_segs:
                seq = next(iter(self._ready_segs))
                if seq >= floor:
                    break
                del self._ready_segs[seq]
                self._window_meta.pop(seq, None)
                self._window_delta.pop(seq, None)
                self._shift_cache.pop(seq, None)

    def release(self):
        """Drop everything this channel holds in RAM. Only for a closed channel nobody will read again (a VOD
        whose player has left); the state itself stays registered until the idle reaper removes it."""
        self.discard_stale_stream()
        with self._lock:
            self._last_segment_out = None
            self._last_playlist_out.clear()
            self._video_preload.clear()
            self._video_first_playlist = None

    def close_and_release(self, channel_id: str):
        """close(), then release() once the pre-fetch threads are really gone (they own the buffers until then)."""
        stopped = self.close()
        deadline = time.monotonic() + 20   # a thread can be mid-ffmpeg or mid-fetch; give it time to notice the stop
        while not stopped and time.monotonic() < deadline:
            time.sleep(0.5)
            stopped = not any(t is not None and t.is_alive()
                              for t in (self._audio_prefetch_thread, self._video_prefetch_thread))
        if stopped:
            self.release()
            logger.debug('%s: closed, buffers released', channel_id)

    def close(self) -> bool:
        """Signal all pre-fetch threads to stop and wait for them to exit. Returns True if they all did.

        A thread only notices _audio_prefetch_stop/_video_prefetch_stop
        between blocking calls - one stuck in a slow or hanging _fetch()
        (network I/O has no hard upper bound beyond whatever timeout that
        specific call passed, which is not guaranteed to be <=5s - see
        _fetch's own docstring) won't see the stop signal until that call
        returns. join(timeout=5) can therefore expire while the thread is
        still genuinely running - previously silent, so a caller had no way
        to know cleanup didn't actually finish. Logged here instead of
        swallowed so a thread left running past this point (still holding
        whatever connection/resource its in-flight fetch is using) is at
        least visible for diagnosing exactly this class of resource leak.
        """
        self._audio_prefetch_stop.set()
        self._video_prefetch_stop.set()
        stopped = True
        for t in (self._audio_prefetch_thread, self._video_prefetch_thread):
            if t is not None and t.is_alive():
                t.join(timeout=5)
                if t.is_alive():
                    stopped = False
                    logger.debug('%s: did not stop within 5s of close() - still running, '
                                 'likely blocked in a slow/hanging fetch; left as a detached '
                                 'daemon thread rather than blocking close() further', t.name)
        return stopped

    def start_audio_prefetch_if_needed(self, channel_id: str):
        """Idempotently start the audio pre-fetch thread, if not already running.

        A live viewer (_handle_master) and a recording with no live viewer
        (_ensure_pipeline) can both reach this for the same channel_id from
        independent HTTP handler threads. Without the lock covering the
        is_alive() check and the thread creation together, both could
        observe "not running yet" and each start their own thread, leaving
        two audio pre-fetch loops racing to fill the same ring.
        """
        with self._lock:
            if self._audio_prefetch_thread is not None and self._audio_prefetch_thread.is_alive():
                return
            t = threading.Thread(
                target=_audio_prefetch_loop, args=(channel_id, self), daemon=True,
                name=f'AudioPF-{channel_id[:8]}',
            )
            self._audio_prefetch_thread = t
            t.start()

    def start_video_prefetch_if_needed(self, channel_id: str, variant_idx: int):
        """Idempotently start the video pre-fetch+mux thread, if not already running.

        See start_audio_prefetch_if_needed - same race, same fix, for the
        thread that owns video_epoch. Two independent video pre-fetch
        threads for one channel would each keep their own local video_epoch
        and both call set_video_epoch(), corrupting the shared counter.
        """
        with self._lock:
            if self._video_prefetch_thread is not None and self._video_prefetch_thread.is_alive():
                return
            vt = threading.Thread(
                target=_video_prefetch_loop, args=(channel_id, self, variant_idx), daemon=True,
                name=f'VideoPF-{channel_id[:8]}',
            )
            self._video_prefetch_thread = vt
            vt.start()


_state_lock = threading.Lock()
_channels: dict = {}    # channel_id -> _ChannelState

_rec_clients_lock = threading.Lock()
_rec_clients: 'dict[str, int]' = {}   # channel_id -> number of open /rec/ connections


@contextmanager
def recording_client(channel_id: str):
    """Count one open /rec/ connection on *channel_id* for the duration of the with block.

    Enigma2 starts recordings through nav.recordService() - record timers and TimeshiftCockpit
    alike - and only a record timer is visible in RecordTimer.timer_list, so the open connection
    is the one signal that covers every kind of recorder (see StreamProxy._isRecording).
    """
    with _rec_clients_lock:
        _rec_clients[channel_id] = _rec_clients.get(channel_id, 0) + 1
    try:
        yield
    finally:
        with _rec_clients_lock:
            left = _rec_clients.get(channel_id, 1) - 1
            if left > 0:
                _rec_clients[channel_id] = left
            else:
                _rec_clients.pop(channel_id, None)


def is_recording_active(channel_id: str) -> bool:
    """True while at least one recorder (timer, timeshift, ...) is connected to /rec/ for *channel_id*."""
    with _rec_clients_lock:
        return channel_id in _rec_clients


def _get_state(channel_id: str) -> '_ChannelState | None':
    with _state_lock:
        state = _channels.get(channel_id)
    if state is not None:
        state.last_access = time.monotonic()
    return state


def _ensure_pipeline(channel_id: str, state: '_ChannelState') -> bool:
    """Make sure the fetch/prefetch pipeline for *channel_id* is running.

    Live playback drives /master then /pl itself (GStreamer following the
    rewritten HLS playlist), which starts the audio/video pre-fetch threads
    as a side effect of _handle_master/_handle_playlist. RecordingProxyUnmuxed
    connects straight to /rec/{channel_id}.ts and never touches /master or
    /pl, so a recording started with no concurrent live viewer would
    otherwise never start those threads - state._ready_segs would stay
    empty forever and the recording would get no data. This is called once
    per /rec/ connection to guarantee the pipeline is up regardless of
    whether a live viewer ever triggered it.

    Idempotent and safe to call repeatedly. Returns True if the channel
    needs muxing (demuxed audio); False if it's already a single continuous
    rendition (nothing for this proxy to do) or the master fetch failed.

    Gated on state.variant_urls being empty, not on the video prefetch
    thread's is_alive(): thread liveness alone left a narrow window - any
    /rec/ reconnect (recording retries, HTTP keep-alive drops, ...) calls
    this again, and if it landed in the brief gap between the thread dying
    and being noticed dead, execution fell through to the unconditional
    re-fetch+overwrite below, silently swapping state.audio_url/
    state.variant_urls for freshly-signed CDN URLs out from under an
    already-running live viewer's prefetch threads mid-stream - the same
    class of bug a provider avoids by asking active_channel_url
    first, just here instead: this function only exists
    because RecordingProxyUnmuxed.handle_recording() calls it on every
    connection, live playback never goes through it at all.

    Once variant_urls is populated at all, restarting a dead thread needs
    no fresh fetch - start_audio_prefetch_if_needed/
    start_video_prefetch_if_needed are already independently idempotent
    (their own is_alive() check under state._lock) and the loops they
    start read state.variant_urls/state.audio_url live, so calling them
    again here with the existing values is exactly what a dead-thread
    restart needs, without touching URLs a live viewer might depend on.
    """
    # A recorder reconnecting after the live slot closed this channel (zapping away or playing a
    # timeshift file) finds the stop Events still set; revive the pipeline or the threads below exit at once.
    if state.reopen():
        logger.debug('%s: pipeline was closed, reopened for recording', channel_id)
    if state.variant_urls:
        if state.audio_url:
            state.start_audio_prefetch_if_needed(channel_id)
        state.start_video_prefetch_if_needed(channel_id, 0)
        return True

    if not resolved_master_url(state):
        return False
    text = _fetch(state.master_url, tag='ensure-pipeline:master', headers=state.headers)
    if not text and _can_refresh(state):
        fresh_url = state.url_refresher()
        if fresh_url:
            state.master_url = fresh_url
            text = _fetch(state.master_url, tag='ensure-pipeline:master-refreshed', headers=state.headers)
    if not text:
        return False

    if not HLSPlaylist.needs_audio_mux(text):
        return False

    master_base = _base_url(text, state.master_url)
    audio_candidates = []
    variant_urls = []
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith('#EXT-X-MEDIA:'):
            if 'TYPE=AUDIO' in line:
                cand = HLSPlaylist.audio_candidate(line, master_base)
                if cand:
                    audio_candidates.append(cand)
            i += 1
            continue
        if line.startswith('#EXT-X-STREAM-INF:'):
            i += 1
            if i < len(lines):
                variant_urls.append(urljoin(master_base, lines[i]))
                i += 1
            continue
        i += 1

    audio_url = HLSPlaylist.select_primary_audio(audio_candidates)
    with state._lock:
        state.audio_url = audio_url
        state.variant_urls = variant_urls

    if audio_url:
        state.start_audio_prefetch_if_needed(channel_id)

    if variant_urls:
        state.start_video_prefetch_if_needed(channel_id, 0)

    logger.debug('%s: recording-only bootstrap started prefetch pipeline (%s variant(s))',
                 channel_id, len(variant_urls))
    return True


# ── Continuous-buffer window cutting ─────────────────────────────────────────

def _prune_audio_bufs(state: '_ChannelState'):
    """Bound state._audio_bufs to the current epoch's buffer plus
    _AUDIO_BUFFER_RETAIN_EPOCHS closed ones, additionally dropping any
    closed buffer older than seg_duration * _LATE_AUDIO_WAIT_FACTOR -
    mirrors the depth/age bounds the old audio ring (_MAX_AUDIO_RING /
    _MAX_AUDIO_EPOCH_LAG / add_audio's max_age pruning) used to enforce.

    Also caps every buffer - open or closed - to _MAX_AUDIO_BUFFER_BYTES
    regardless of whether it's ever been successfully matched: the
    depth/age bounds above only ever apply to a *closed* buffer, and
    trim_before() only ever shrinks one in response to a successful cut,
    so the *current* (open) buffer would otherwise grow without bound for
    as long as it keeps failing to match anything (see
    _MAX_AUDIO_EPOCH_LAG's resync, which should normally prevent that
    situation from lasting long enough for this cap to matter at all).

    Caller must hold state._lock.
    """
    for b in state._audio_bufs:
        b.trim_to_max_bytes(_MAX_AUDIO_BUFFER_BYTES)
    max_age = state.seg_duration * _LATE_AUDIO_WAIT_FACTOR
    now = time.monotonic()
    kept = []
    closed_kept = 0
    unconsumed_kept = 0
    video_epoch = state._video_epoch   # plain read: caller holds state._lock
    for b in state._audio_bufs:
        if b.closed_at is None:
            kept.append(b)
            continue
        # A closed audio buffer the video side hasn't reached yet (its
        # epoch is the video epoch or later, allowing for the one-epoch-back
        # fallback) is still needed, however old or however many of them
        # there are: at a cold start the audio pre-population runs several
        # transitions ahead of the first video windows (a stitched ad
        # pre-roll), and the age/count limits below would otherwise evict
        # the audio for epochs video hasn't cut yet, leaving every one of
        # those windows without audio. Capped as a backstop.
        if b.epoch >= video_epoch - 1 and unconsumed_kept < _MAX_UNCONSUMED_AUDIO_EPOCHS:
            kept.append(b)
            unconsumed_kept += 1
            continue
        if now - b.closed_at > max_age:
            continue
        if closed_kept >= _AUDIO_BUFFER_RETAIN_EPOCHS:
            continue
        kept.append(b)
        closed_kept += 1
    state._audio_bufs = kept


def _append_audio(state: '_ChannelState', channel_id: str, seq: int, disc: bool, data: bytes):
    """Append a freshly-fetched, decrypted audio segment's bytes into the
    current (or a freshly-opened) audio _RenditionBuffer.

    Mirrors the old assign_audio_epoch's exactly-once-per-seq disc dedup
    (see _SEQ_REPROCESS_WINDOW) via state._audio_disc_seqs_seen, and the old
    add_audio's already-queued dedup (both the one-shot playlist
    pre-population pass and the ongoing background audio pre-fetch thread
    parse the same playlist independently and both start from the first
    segment) via state._audio_appended_seqs - without the latter, the two
    could append the same seq's bytes twice into a buffer, corrupting its
    PTS index.

    Callers must invoke this in strictly ascending *seq* order relative to
    each other (see _audio_prefetch_loop's and _handle_playlist's own
    ordering) - _RenditionBuffer.append requires appends to arrive in PTS
    order to keep its index valid; unlike the old flat, order-independent
    audio ring, a continuous buffer cannot tolerate out-of-order writes.

    The expensive PTS/PAT-PMT scan runs before state._lock is taken (see
    _scan_ts_index's own docstring for why).
    """
    pts_offsets, pat_pmt = _scan_ts_index(data)
    if not pts_offsets:
        logger.debug('%s/%s: Event: no PTS found in audio segment'
                     ' | Action: drop, not appended to buffer', channel_id, seq)
        return
    with state._lock:
        if seq in state._audio_appended_seqs:
            return
        state._audio_appended_seqs.add(seq)
        state._audio_appended_seqs = {
            s for s in state._audio_appended_seqs if s >= seq - _SEQ_REPROCESS_WINDOW
        }
        if disc and seq not in state._audio_disc_seqs_seen:
            state._audio_epoch += 1
            for b in state._audio_bufs:
                b.mark_closed()
            state._audio_bufs.insert(0, _RenditionBuffer(state._audio_epoch, is_disc_epoch=True))
        state._audio_disc_seqs_seen.add(seq)
        state._audio_disc_seqs_seen = {
            s for s in state._audio_disc_seqs_seen if s >= seq - _SEQ_REPROCESS_WINDOW
        }
        if not state._audio_bufs:
            state._audio_bufs.insert(0, _RenditionBuffer(state._audio_epoch, is_disc_epoch=False))
        state._audio_bufs[0].append(data, pts_offsets, pat_pmt)
        _prune_audio_bufs(state)


def _slice_audio_for_window(state: '_ChannelState', epoch: int, start_pts: int, end_pts: int,
                            is_disc_window: bool, wait_secs: 'float | None' = None,
                            channel_id: str = '', chunk_seq: 'int | None' = None
                            ) -> 'tuple[bytes | None, int | None, bool]':
    """Slice audio bytes covering [start_pts, end_pts) for one output window.

    Replaces pop_audio_for_pts's discrete nearest-match search: searches
    state._audio_bufs for a buffer tagged *epoch* (or one epoch back,
    unless *is_disc_window* - the same reset-PTS coincidence-risk exemption
    pop_audio_for_pts gated via allow_stale_epoch) whose continuous PTS
    index already covers the full requested range, waiting up to
    *wait_secs* for a buffer to catch up. *is_disc_window* only affects the
    stale-epoch gating here - callers pass their own explicit *wait_secs*
    for the disc case (see _cut_and_mux_next_window's short bounded
    _DISC_AUDIO_PEEK_WAIT and _resolve_late_audio_window's much longer
    _LATE_AUDIO_WAIT_FACTOR budget); the state.seg_duration*1.6 default
    below only actually applies to an ordinary, non-disc window whose
    caller didn't specify one. Returns (sliced_bytes, actual_a_start_pts,
    True) on success - the middle value lets a caller feed _ffmpeg_mux's
    itsoffset calculation directly instead of re-deriving it with a second
    _first_pts scan over bytes already fully indexed here - or
    (None, None, False) on timeout.

    Unlike the old segment-granularity match, there is no "nearest but
    outside tolerance" concept here: either some buffer's continuous index
    already spans the exact range needed, or it doesn't yet (worth
    retrying) or never will (a genuine content gap - see silence-injection
    fallback in _ffmpeg_mux).
    """
    if wait_secs is None:
        wait_secs = state.seg_duration * 1.6
    deadline = time.monotonic() + wait_secs
    stale_ok_at = time.monotonic() + min(_STALE_AUDIO_DELAY, wait_secs / 2)
    snapshot = ''   # last-seen state of every held buffer, for the miss log below
    audio_moved_on = False
    while True:
        with state._lock:
            candidates = [b for b in state._audio_bufs if b.epoch == epoch]
            if not is_disc_window and time.monotonic() >= stale_ok_at:
                candidates += [b for b in state._audio_bufs if b.epoch == epoch - 1]
            for b in candidates:
                cut = b.cut(start_pts, end_pts)
                if cut is not None and b.epoch != epoch and (cut[1] - start_pts > _STALE_COVER_SLACK
                                                             or end_pts - cut[2] > _STALE_COVER_SLACK):
                    cut = None
                if cut is not None:
                    data, a_start, _a_end = cut
                    b.trim_before(state.audio_trim_floor(b.epoch, a_start))
                    if channel_id:
                        stale = ' (stale-epoch fallback)' if b.epoch != epoch else ''
                        logger.debug('%s/%s: audio window matched epoch=%s pts=[%s,%s)%s',
                                     channel_id, chunk_seq, b.epoch, start_pts, end_pts, stale)
                    return data, a_start, True
            # Captured every iteration (cheap - just a few ints per buffer,
            # not the buffer contents) so a subsequent miss can log what was
            # actually held instead of just "nothing matched": lets a miss
            # be diagnosed directly from this one line - was there truly
            # nothing nearby (genuine content gap upstream, e.g. an ad
            # creative whose audio track starts later than its video), or
            # did a buffer exist but not yet reach the needed PTS (worth
            # retrying) - without having to cross-reference the next
            # window's own match log to reconstruct it by hand.
            snapshot = ', '.join(
                f'epoch={b.epoch} pts=[{b.pts_list[0]},{b.pts_list[-1]}] n={len(b.pts_list)}'
                if b.pts_list else f'epoch={b.epoch} empty'
                for b in state._audio_bufs
            ) or 'no buffers held'
            # The audio rendition went through a transition the video one hasn't flagged (yet, or ever): every
            # buffer this window could draw on is closed and a later epoch exists. A closed buffer is never
            # extended, so waiting for the rest of wait_secs cannot produce this window's audio.
            audio_moved_on = (bool(candidates) and all(b.closed_at is not None for b in candidates)
                              and any(b.epoch > epoch for b in state._audio_bufs))
        if audio_moved_on or time.monotonic() >= deadline:
            if channel_id:
                logger.debug('%s/%s: audio MISS window epoch=%s pts=[%s,%s) waited=%.2fs'
                             ' is_disc_window=%s%s buffers=[%s]',
                             channel_id, chunk_seq, epoch, start_pts, end_pts,
                             wait_secs - max(0.0, deadline - time.monotonic()),
                             is_disc_window, ' (audio already moved on to a later epoch)' if audio_moved_on else '',
                             snapshot)
            return None, None, False
        if state._audio_prefetch_stop.wait(timeout=0.1):
            return None, None, False   # channel closed: the audio will never arrive, don't hold close() up


def _resolve_late_audio_window(channel_id: str, state: '_ChannelState', chunk_seq: int,
                               video_slice: bytes, epoch: int, start_pts: int, end_pts: int,
                               is_disc_window: bool):
    """Background retry for a window whose audio wasn't covered yet at cut
    time - replaces _resolve_late_audio, retargeted at a PTS range instead
    of a single discrete segment match.

    Runs off the critical path with the extended _LATE_AUDIO_WAIT_FACTOR
    budget, giving the audio rendition's own disc tag (or just ordinary CDN
    fetch lag) extra time to catch up; if the range becomes coverable in
    time and Enigma2 hasn't already been served the silence version, it
    re-muxes and overwrites state._ready_segs[chunk_seq] in place.
    """
    try:
        aslice, a_pts, covered = _slice_audio_for_window(
            state, epoch, start_pts, end_pts, is_disc_window,
            wait_secs=state.seg_duration * _LATE_AUDIO_WAIT_FACTOR,
            channel_id=channel_id, chunk_seq=chunk_seq)
        with state._lock:
            if chunk_seq not in state._ready_segs:
                return   # already served – too late to improve
        if not covered:
            logger.debug('%s/%s: Event: no audio ever covered this window'
                         ' (kept as silence) | epoch=%s pts=[%s,%s)',
                         channel_id, chunk_seq, epoch, start_pts, end_pts)
            return
        # is_disc must reflect whether *this window* is actually a real
        # content transition (is_disc_window), not be hardcoded True: this
        # same retry path now also fires for an ordinary window whose audio
        # simply lagged (ordinary CDN fetch delay, not an ad break) - see
        # _cut_and_mux_next_window's DISC_AUDIO_PEEK_WAIT-vs-None wait_secs
        # split. Forcing is_disc=True there would plant a false
        # -mpegts_flags initial_discontinuity into an otherwise-continuous
        # segment, which GStreamer's hlsdemux and RecordingProxyUnmuxed's
        # _segment_is_disc both trust as "a real transition happened here".
        muxed = _ffmpeg_mux(video_slice, aslice, state, is_disc=is_disc_window, track_cc=False,
                            v_pts=start_pts, a_pts=a_pts)
        with state._lock:
            if chunk_seq in state._ready_segs:
                state._ready_segs[chunk_seq] = muxed
                # See _resolve_late_audio's original comment (now here): the
                # bytes actually served for chunk_seq just changed, so
                # state._cc_state's baseline (still whatever the discarded
                # silence version ended at, since this call deliberately
                # didn't chain into it - track_cc=False above) needs the
                # *next* window's mux to restart from a fresh CC baseline
                # without also flagging it as its own content transition.
                # Unconditional regardless of is_disc_window: this fixes a
                # real CC-continuity mismatch (the served bytes changed
                # size), not a content-transition flag.
                state._cc_resync_pending = True
                logger.debug('%s/%s: late-audio window upgrade applied', channel_id, chunk_seq)
    finally:
        state.clear_provisional(chunk_seq)
        state.remove_pending_audio_floor(epoch, start_pts)


def _cut_and_mux_next_window(channel_id: str, state: '_ChannelState') -> bool:
    """Cut one output window covering everything currently buffered in
    state._video_buf from the cursor on, slice matching audio, mux, and
    publish to state._ready_segs.

    Replaces the old per-CDN-segment mux call in _video_prefetch_loop.
    Deliberately cuts by *CDN segment boundary*, not by a fixed target
    duration: an HLS video segment is guaranteed self-contained (its own
    SPS/PPS and a leading IDR/keyframe), which is exactly why the old
    per-segment design never had a decode problem on the video side in the
    first place. Cutting at an arbitrary fixed-duration PTS boundary
    instead (an earlier version of this function did exactly that) breaks
    that guarantee: a window frequently starts mid-GOP with no leading
    keyframe, and sometimes with no SPS/PPS NAL in range at all - confirmed
    in production as periodic video freezes (decoder has nothing
    displayable until the next real keyframe) and outright _ffmpeg_mux
    failures ("non-existing PPS 0 referenced", rc=234, 0 bytes output,
    silently falling back to video-only - the exact video_data-only path
    _ffmpeg_mux's docstring already flags as worse than the mismatch it
    exists to paper over). Cutting at CDN segment boundaries restores that
    guarantee while still sourcing audio from the continuous buffer for
    whatever exact PTS range this video segment covers - audio no longer
    needs to be a matching *discrete segment*, just a PTS-range slice,
    which is the actual fix the windowed-buffer redesign was for; nothing
    about that benefit depends on how video's own boundaries are chosen.

    state._video_buf/_video_cursor_pts are touched only by the video
    prefetch thread (this function is only ever called synchronously from
    that same thread), so - like state._cc_state - they need no lock of
    their own; only the state._ready_segs/_window_meta/_next_chunk_seq
    publish step below is shared with HTTP handler threads and
    _resolve_late_audio_window, and is locked accordingly.

    Returns True if a window was produced, False if there was nothing new
    to cut (e.g. called again with no new append since the last call).
    """
    vbuf = state._video_buf
    cursor = state._video_cursor_pts
    if vbuf is None or cursor is None:
        return False
    cut = vbuf.cut(cursor, None)
    if cut is None:
        return False
    vslice, actual_start, actual_end = cut
    if actual_end <= actual_start:
        return False

    is_disc = vbuf.is_disc_epoch and not vbuf.first_window_emitted
    vbuf.first_window_emitted = True

    # The audio of consecutive windows has to join up: a window's audio range ends at the pts of its last video
    # entry, and the next one used to start at the pts of its first, a little later (21-40 ms). The audio packets
    # (several frames each) that start in that gap were in neither window: about 16 ms of audio lost per window on
    # average (Hogan's Heroes: 4.984 s of audio per 5 s window). Enigma2 plays audio on without regard to such small
    # holes in the timestamps, so the audio ran ahead of the picture, a little more with every window, until the
    # next discontinuity made it synchronise again. Continue where the previous window's audio ended instead.
    a_start = actual_start
    prev_end = state._audio_slice_end
    if (not is_disc and prev_end is not None and prev_end[0] == vbuf.epoch
            and 0 < actual_start - prev_end[1] <= _AUDIO_JOIN_MAX_GAP):
        a_start = prev_end[1]
    state._audio_slice_end = (vbuf.epoch, actual_end)

    aslice, a_pts, covered = _slice_audio_for_window(
        state, vbuf.epoch, a_start, actual_end, is_disc_window=is_disc,
        wait_secs=(_DISC_AUDIO_PEEK_WAIT if is_disc else None),
        channel_id=channel_id)

    # reset_cc honors a pending late-audio-upgrade resync (see
    # _resolve_late_audio_window) - kept separate from is_disc itself so a
    # bookkeeping-only CC-baseline restart never plants a false
    # -mpegts_flags initial_discontinuity in an otherwise-ordinary window.
    reset_cc = is_disc
    with state._lock:
        if state._cc_resync_pending:
            reset_cc = True
            state._cc_resync_pending = False
    muxed = _ffmpeg_mux(vslice, aslice, state, is_disc=is_disc, track_cc=True, reset_cc=reset_cc,
                        v_pts=actual_start, a_pts=a_pts)

    with state._lock:
        chunk_seq = state._next_chunk_seq
        state._next_chunk_seq += 1
        state._ready_segs[chunk_seq] = muxed
        state._window_meta[chunk_seq] = _WindowMeta(vbuf.epoch, actual_start, actual_end, is_disc)
        state.assign_live_delta(chunk_seq, actual_start, actual_end, is_disc)
        while len(state._ready_segs) > _MAX_SEG_CACHE:
            k, _ = state._ready_segs.popitem(last=False)
            state._window_meta.pop(k, None)

    logger.debug('%s/%s: Event: window cut epoch=%s pts=[%s,%s) is_disc=%s audio_covered=%s'
                 ' | Action: mux (%s bytes)',
                 channel_id, chunk_seq, vbuf.epoch, actual_start, actual_end, is_disc, covered,
                 len(muxed))

    if not covered:
        state.mark_provisional(chunk_seq)
        # Registered here, synchronously, not inside the spawned thread -
        # see add_pending_audio_floor's own comment for why that gap would
        # matter (a concurrent trim landing between "published uncovered"
        # and "retry thread actually running" would slip through otherwise).
        state.add_pending_audio_floor(vbuf.epoch, a_start)
        threading.Thread(
            target=_resolve_late_audio_window,
            args=(channel_id, state, chunk_seq, vslice, vbuf.epoch, a_start, actual_end, is_disc),
            daemon=True,
            name=f'LateAudio-{channel_id[:8]}-{chunk_seq}',
        ).start()

        # A miss is routine right at a real transition (the audio
        # rendition's own disc tag landing a segment or two late - the
        # late-audio retry above exists exactly to ride that out). It's
        # only a genuine, persistent divergence - the video rendition
        # flagging a transition the audio rendition never mirrored, or vice
        # versa - once the gap reaches _MAX_AUDIO_EPOCH_LAG: video_epoch and
        # audio_epoch are bumped independently, by two different threads
        # parsing two different CDN renditions' own #EXT-X-DISCONTINUITY
        # tags, with nothing else cross-checking them against each other.
        # Without this, once they drift apart by _MAX_AUDIO_EPOCH_LAG or
        # more, _slice_audio_for_window's epoch-or-one-back match can never
        # succeed again for the rest of the channel session - confirmed in
        # production (pre-windowed-mux design, same underlying mechanism)
        # as a permanent, total silence outage once this happened, with
        # nothing left to bring the two counters back in sync on their own.
        audio_epoch_now = state.current_audio_epoch()
        if audio_epoch_now > vbuf.epoch:
            logger.debug('%s/%s: Event: audio_epoch already ahead of video_epoch'
                         ' (video rendition never flagged this transition) | Action:'
                         ' resync video_epoch %s -> %s',
                         channel_id, chunk_seq, vbuf.epoch, audio_epoch_now)
            vbuf.mark_closed()
            state._video_buf = _RenditionBuffer(audio_epoch_now, is_disc_epoch=False)
            state._video_cursor_pts = None
            state.set_video_epoch(audio_epoch_now)
            # Fresh buffer has no real continuity relationship to whatever
            # state._cc_state was chained from - see _cc_resync_pending's
            # own comment (same reasoning as the thread-respawn case).
            # Deliberately CC-only, not is_disc=True: this is a bookkeeping
            # correction, not a confirmed real content transition (only
            # audio's own tag is evidence here, not video's).
            with state._lock:
                state._cc_resync_pending = True
            return True
        if vbuf.epoch - audio_epoch_now >= _MAX_AUDIO_EPOCH_LAG:
            logger.debug('%s/%s: Event: audio_epoch persistently behind video_epoch'
                         ' by >= %s (missed audio-side disc marker) | Action:'
                         ' resync video_epoch %s -> %s',
                         channel_id, chunk_seq, _MAX_AUDIO_EPOCH_LAG, vbuf.epoch, audio_epoch_now)
            vbuf.mark_closed()
            state._video_buf = _RenditionBuffer(audio_epoch_now, is_disc_epoch=False)
            state._video_cursor_pts = None
            state.set_video_epoch(audio_epoch_now)
            with state._lock:
                state._cc_resync_pending = True
            return True

    # Not actual_end: cut() deliberately holds back vbuf's own current last
    # indexed entry (see its own comment) so the next cut's start_i lands on
    # that still-unreleased entry instead of re-matching the one just cut.
    state._video_cursor_pts = vbuf.pts_list[-1]
    vbuf.trim_before(actual_start, retain_bytes=_VIDEO_BUFFER_RETAIN_MARGIN_BYTES)
    return True


def _estimate_v_end_pts(all_pts: list) -> int:
    """Estimate a VOD video segment's own end PTS from its own frame
    spacing (the *most common* gap between consecutive distinct PTS
    values, sorted - robust to B-frame transmission-order reordering).

    Only a fallback for when no subsequent segment's real PTS is
    available to give an exact boundary instead - the final segment of a
    VOD asset, or one immediately preceding a real CDN discontinuity.
    Confirmed by directly comparing consecutive real windows' own logged
    PTS ranges in a production session that this estimate systematically
    overshoots the next segment's true start by ~40-55ms on *every*
    boundary - not occasional noise, every single one - which is exactly
    why _video_prefetch_loop now defers muxing a segment until its actual
    successor's own v_pts is known, using this only where no successor
    will ever exist to ask instead.
    """
    distinct_sorted = sorted(set(all_pts))
    if len(distinct_sorted) > 1:
        deltas = [b - a for a, b in zip(distinct_sorted, distinct_sorted[1:])]
        delta_counts: dict = {}
        for _d in deltas:
            delta_counts[_d] = delta_counts.get(_d, 0) + 1
        frame_interval = max(delta_counts, key=delta_counts.get)
    else:
        frame_interval = 1
    return max(all_pts) + frame_interval


def _mux_whole_video_segment(channel_id: str, state: '_ChannelState', epoch: int,
                             vdata: bytes, v_pts: int, v_end_pts: int, is_disc: bool,
                             pending_disc_ahead: bool = False):
    """VOD-only alternative to _cut_and_mux_next_window: mux one whole CDN
    video segment as its own output window, instead of slicing a sub-range
    out of a continuous buffer via _RenditionBuffer.cut().

    _RenditionBuffer.cut() locates its byte range with bisect() over
    pts_list, which requires pts_list to be sorted - true for audio (AAC
    has no B-frames, so transmission order already equals display order)
    but not guaranteed for H.264 video with B-frames: a P-frame is
    transmitted before the B-frames that display earlier than it, so a
    video segment's own pts_offsets (as scanned by _scan_ts_index, in
    transmission order) can be locally out of order. Confirmed on real
    FAST-channel content via a diagnostic in _RenditionBuffer.append(): up to
    roughly half the PTS entries in a single segment out of order, on both
    live and VOD channels equally. bisect() on unsorted input doesn't
    raise - it silently returns a technically-computed but wrong index,
    slicing the wrong byte range. Only VOD is switched to this whole-
    segment path (see the call site in _video_prefetch_loop) rather than
    fixing _RenditionBuffer.cut() itself, to keep live TV's already-working
    code path completely untouched.

    None of this needs fixing in the first place for the common case this
    function actually handles: in steady state, _video_prefetch_loop calls
    this once per newly-fetched CDN segment, and an HLS segment is already
    self-contained (its own SPS/PPS + leading keyframe) - there is no
    reason to slice it out of a bigger buffer by PTS at all. Muxing it
    whole, exactly as fetched, sidesteps the bisect dependency entirely.

    *v_pts* and *v_end_pts* must be this segment's true min/max PTS
    (order-independent - see the B-frame reasoning above; the caller
    computes both via min()/max() over every entry in this segment's own
    pts_offsets), not an approximated span. An earlier version of this
    function derived v_end_pts as v_pts + one nominal seg_duration instead
    - which is exactly the kind of imprecision the windowed-buffer redesign
    (see _cut_and_mux_next_window's own docstring on why it replaced the
    old per-segment design's tolerance-based matching with exact PTS-range
    coverage) moved away from for good reason: a real CDN segment's true
    span is never exactly the nominal target duration, so that
    approximation systematically over- or under-covers the audio query by
    a small amount on *every* window. cut()'s exact-range matching still
    "succeeds" either way (over-coverage doesn't fail the match, it just
    slices more bytes), so nothing here ever shows up as a miss or a large
    per-window itsoffset - but over hundreds of windows in a long VOD
    title, a systematic few-millisecond-per-window bias compounds into a
    multi-second, steadily growing A/V offset that survives seeking
    (every window carries the same bias) and never self-corrects (nothing
    re-anchors to true content position - each window's own start is
    exact, via min(), but its end, and therefore how much of the *next*
    window's rightful audio bleeds into this one, was not). Using this
    segment's own true max() instead removes the systematic bias entirely.

    Mirrors _cut_and_mux_next_window's own audio-matching, muxing,
    publishing, provisional/late-audio-retry, and epoch-resync logic
    exactly (see that function's comments for the reasoning behind each),
    just operating on a whole fetched segment instead of a buffer slice.
    """
    aslice, a_pts, covered = _slice_audio_for_window(
        state, epoch, v_pts, v_end_pts, is_disc_window=is_disc,
        wait_secs=(_DISC_AUDIO_PEEK_WAIT if is_disc else None),
        channel_id=channel_id)

    reset_cc = is_disc
    with state._lock:
        if state._cc_resync_pending:
            reset_cc = True
            state._cc_resync_pending = False
    muxed = _ffmpeg_mux(vdata, aslice, state, is_disc=is_disc, track_cc=True, reset_cc=reset_cc,
                        v_pts=v_pts, a_pts=a_pts)

    with state._lock:
        chunk_seq = state._next_chunk_seq
        state._next_chunk_seq += 1
        state._ready_segs[chunk_seq] = muxed
        state._window_meta[chunk_seq] = _WindowMeta(epoch, v_pts, v_end_pts, is_disc)
        state.assign_live_delta(chunk_seq, v_pts, v_end_pts, is_disc)
        while len(state._ready_segs) > _MAX_SEG_CACHE:
            k, _ = state._ready_segs.popitem(last=False)
            state._window_meta.pop(k, None)

    logger.debug('%s/%s: Event: whole-segment window epoch=%s pts=[%s,%s) is_disc=%s'
                 ' audio_covered=%s | Action: mux (%s bytes)',
                 channel_id, chunk_seq, epoch, v_pts, v_end_pts, is_disc, covered, len(muxed))

    if not covered:
        state.mark_provisional(chunk_seq)
        # Registered here, synchronously, not inside the spawned thread -
        # see add_pending_audio_floor's own comment for why that gap would
        # matter (a concurrent trim landing between "published uncovered"
        # and "retry thread actually running" would slip through otherwise).
        state.add_pending_audio_floor(epoch, v_pts)
        threading.Thread(
            target=_resolve_late_audio_window,
            args=(channel_id, state, chunk_seq, vdata, epoch, v_pts, v_end_pts, is_disc),
            daemon=True,
            name=f'LateAudio-{channel_id[:8]}-{chunk_seq}',
        ).start()

        # See _cut_and_mux_next_window's own comment for the reasoning
        # behind this resync - identical here, just reading/writing
        # state.set_video_epoch() directly instead of also replacing a
        # _video_buf this path doesn't use.
        audio_epoch_now = state.current_audio_epoch()
        # pending_disc_ahead guards only this branch (resync *up*) - see
        # _DISC_LOOKAHEAD_SEGMENTS's own comment. A pending tag means video
        # is about to flag this same transition itself moments later in
        # this same loop; resyncing here too would double-bump video_epoch
        # for one real transition. Not applied to the resync-*down* branch
        # below: that one fires only after _MAX_AUDIO_EPOCH_LAG segments of
        # persistent lag, long past any single pending tag being the cause.
        if audio_epoch_now > epoch and not pending_disc_ahead:
            logger.debug('%s/%s: Event: audio_epoch already ahead of video_epoch'
                         ' (video rendition never flagged this transition) | Action:'
                         ' resync video_epoch %s -> %s',
                         channel_id, chunk_seq, epoch, audio_epoch_now)
            state.set_video_epoch(audio_epoch_now)
            with state._lock:
                state._cc_resync_pending = True
        elif epoch - audio_epoch_now >= _MAX_AUDIO_EPOCH_LAG:
            logger.debug('%s/%s: Event: audio_epoch persistently behind video_epoch'
                         ' by >= %s (missed audio-side disc marker) | Action:'
                         ' resync video_epoch %s -> %s',
                         channel_id, chunk_seq, _MAX_AUDIO_EPOCH_LAG, epoch, audio_epoch_now)
            state.set_video_epoch(audio_epoch_now)
            with state._lock:
                state._cc_resync_pending = True


# ── Audio pre-fetch thread ────────────────────────────────────────────────────

def _audio_prefetch_loop(channel_id: str, state: '_ChannelState'):
    """Background thread: continuously fetch new audio segments and append
    them, in seq order, to the per-channel continuous audio buffer(s).

    Polls the audio rendition playlist, fetches any segments not yet
    appended, decrypts them if necessary, and hands each to _append_audio
    (which resolves/bumps the audio epoch and opens a fresh _RenditionBuffer
    on a real #EXT-X-DISCONTINUITY). Segments are fetched in parallel via
    _AUDIO_FETCH_WORKERS, but appended to the buffer strictly in seq order
    once the whole poll's batch has finished fetching - _RenditionBuffer's
    PTS index requires ascending-PTS appends to stay valid, unlike the old
    flat, order-independent audio ring, which tolerated parallel workers
    finishing in any order.
    """
    # state._audio_bufs is reset, if needed, by register_channel() before
    # this thread is ever started - not here. An earlier version cleared it
    # right here instead, reasoning that a respawned thread's stale open
    # buffer (see register_channel's own comment) needed resetting - true,
    # but this thread's start() is only a *schedule* request, not a
    # guarantee of immediate execution, and _handle_playlist's synchronous
    # audio pre-population can run - and actually append real, fresh
    # segments - before this thread's first line ever gets CPU time.
    # Confirmed in production: the reset here raced and won, wiping out
    # pre-population's just-appended data and leaving _audio_bufs
    # completely empty ("no buffers held") for the critical first seconds
    # instead of just stale - worse than the bug it was meant to fix.
    # register_channel() runs synchronously in the same call that hands
    # Enigma2 the proxy URL, strictly before any request (and therefore
    # before this thread or pre-population) can reach this channel at all,
    # so resetting there has no such race to lose.
    fetched_seqs: set[int] = set()
    stale_aseq_base: 'int | None' = None  # aseq_base last seen with an empty to_fetch
    stale_since: 'float | None' = None    # monotonic time stale_aseq_base was first seen
    idle_log_at = 0.0                     # monotonic time of the last idle-poll diagnostic line

    # Poll immediately on first iteration so the buffer fills as fast as
    # possible, then wait seg_duration / 5 between subsequent polls.
    while True:
        if state._audio_prefetch_stop.is_set():
            break
        audio_url = state.audio_url
        if not audio_url:
            time.sleep(0.1)   # audio_url populated by _handle_master before thread starts; guard against any race
            continue

        atext = _fetch(audio_url, tag='audio-prefetch:playlist', headers=state.headers)
        if not atext:
            continue

        # Diagnostic only - see HLSPlaylist.ad_markers's docstring. Logged
        # unconditionally when present (not deduped across polls) since
        # this is meant to be pulled once a real capture confirms whether
        # a provider sends any of these at all, not left running long-term.
        ad_markers = HLSPlaylist.ad_markers(atext)
        if ad_markers:
            logger.debug('%s: audio playlist ad markers: %s', channel_id, ad_markers)

        abase = audio_url.split('?')[0].rsplit('/', 1)[0] + '/'
        try:
            aseq_base, asegs = HLSPlaylist.segments(atext, abase)
            audio_key_urls, audio_ivs, audio_discs = HLSPlaylist.segment_keys(atext, abase)
        except Exception as exc:
            logger.debug('%s: audio playlist parse error: %s', channel_id, exc)
            continue

        # See the matching call in _video_prefetch_loop: anchors the pacing
        # throttle's floor to this playlist's own reported position so a
        # fresh live zap (whose real media-sequence numbers can already be
        # in the thousands) isn't throttled down to a bogus 0 baseline
        # until Enigma2's first real request lands.
        state.record_segment_request(max(0, aseq_base - 1))

        # Trim fetched_seqs to avoid unbounded growth. Same window as
        # _append_audio's own dedup sets - see _SEQ_REPROCESS_WINDOW.
        fetched_seqs = {s for s in fetched_seqs if s >= aseq_base - _SEQ_REPROCESS_WINDOW}

        # Bounded to segments within _PREFETCH_LEAD_SEGMENTS of what
        # Enigma2 has actually requested (see record_segment_request) - see
        # _video_prefetch_loop's matching throttle for why. `break`, not
        # `continue`+skip, so this poll's batch stops at the same
        # contiguous boundary every time rather than fetching a later
        # segment's disc flag out of order relative to ones still waiting
        # on the bound.
        last_seq_ok = state.last_requested_seq() + _PREFETCH_LEAD_SEGMENTS
        to_fetch = []
        bound_hit_at = None   # first seq withheld by the last_seq_ok bound, if any
        for i, url in enumerate(asegs):
            seq = aseq_base + i
            if seq > last_seq_ok:
                bound_hit_at = seq
                break
            if seq not in fetched_seqs:
                k = audio_key_urls[i] if i < len(audio_key_urls) else None
                iv = audio_ivs[i] if i < len(audio_ivs) else None
                disc = audio_discs[i] if i < len(audio_discs) else False
                to_fetch.append((seq, url, k, iv, disc))

        if not to_fetch:
            if bound_hit_at is not None:
                # last_requested_seq() is lagging aseq_base by more than
                # _PREFETCH_LEAD_SEGMENTS - this loop's own self-anchoring
                # call above (record_segment_request(aseq_base - 1)) should
                # make that impossible, so surface it rather than silently
                # starving forever the way this whole branch otherwise can:
                # observed in production as a live playback session losing
                # audio permanently after a same-channel re-zap, with
                # nothing in the log to say why.
                logger.debug('%s: Event: audio prefetch withheld by lead bound'
                             ' - aseq_base=%s bound_hit_at=%s last_seq_ok=%s'
                             ' last_requested_seq=%s | Action: pair with silence until this clears',
                             channel_id, aseq_base, bound_hit_at, last_seq_ok,
                             state.last_requested_seq())
            # All currently-known audio seqs have already been fetched.
            # The ring being empty here is *not* by itself a sign of a
            # bumper loop: it also happens for a normal poll or two right
            # at a real content transition, while the CDN just hasn't
            # published the next segment yet. Clearing fetched_seqs then
            # would make the next poll re-process the same still-in-window
            # #EXT-X-DISCONTINUITY tag this loop already counted, bumping
            # _audio_epoch a second time for one real transition and
            # permanently desyncing it from video_epoch.
            #
            # A genuine bumper loop instead shows aseq_base itself stuck
            # at the same value poll after poll (the CDN isn't advancing
            # the playlist's media sequence at all), so only clear once
            # that's held for several seg_durations — far longer than the
            # one or two polls a real transition's publish lag ever takes.
            if bound_hit_at is None:
                # Nothing withheld by the lead bound: the playlist itself
                # has nothing newer than what was already fetched.
                now = time.monotonic()
                if now - idle_log_at >= 10.0:
                    idle_log_at = now
                    logger.debug('%s: audio poll idle: aseq_base=%s segments=%s last_seq=%s'
                                 ' endlist=%s fetched_max=%s last_requested_seq=%s',
                                 channel_id, aseq_base, len(asegs), aseq_base + len(asegs) - 1,
                                 '#EXT-X-ENDLIST' in atext,
                                 max(fetched_seqs) if fetched_seqs else None,
                                 state.last_requested_seq())
            if channel_id.startswith('vod'):
                # A VOD audio playlist's media sequence is static (always
                # the same aseq_base) by nature, so "stuck" is meaningless
                # there, and clearing fetched_seqs would only make every
                # poll re-fetch the whole asset's audio from seq 0.
                pass
            elif aseq_base != stale_aseq_base:
                stale_aseq_base = aseq_base
                stale_since = time.monotonic()
            elif time.monotonic() - stale_since >= state.seg_duration * 3:
                logger.debug('%s: Event: audio playlist media sequence stuck'
                             ' at aseq_base=%s for %.1fs | Action: clear fetched_seqs and retry',
                             channel_id, aseq_base, time.monotonic() - stale_since)
                fetched_seqs.clear()
                stale_aseq_base = None
                stale_since = None
            if state._audio_prefetch_stop.wait(timeout=state.seg_duration / 5):
                break
            continue

        stale_aseq_base = None
        stale_since = None

        results: dict = {}   # seq -> decrypted audio bytes; written by worker threads,
        #                    # each to its own distinct key, then consumed in seq order below

        def _fetch_one(seq, url, key_url, iv_explicit):
            # Retries the whole fetch -> decrypt -> parse pipeline up to
            # twice: any one of those stages can intermittently fail on a
            # segment the CDN's own playlist genuinely lists (observed in
            # production: an empty/truncated 200 response for the segment
            # body, a bad/empty key response, or decrypted bytes that fail
            # the sync-byte check) - each used to drop an otherwise-present
            # segment silently and for good, which _resolve_late_audio_window
            # then misreported as "no audio ever covered this window" three
            # log lines later. Piecemeal per-branch retries kept missing new
            # failure shapes as they turned up, so this wraps the whole
            # thing once instead.
            fail_reason = None
            for _attempt in range(2):
                try:
                    adata = _fetch(url, binary=True, timeout=state.seg_duration * 0.8,
                                   tag='audio-prefetch:segment', headers=state.headers)
                    if not adata:
                        fail_reason = 'segment fetch returned no data'
                        continue
                    # Decrypt AES-128-CBC if the segment is encrypted.
                    if key_url:
                        kb = AES128.fetch_key(key_url, _fetcher(state))
                        if not kb:
                            fail_reason = f'key fetch failed for {key_url}'
                            continue
                        adata = AES128.decrypt(adata, kb, AES128.iv(iv_explicit, seq))
                    if not adata or adata[0] != 0x47:
                        fail_reason = 'failed to decrypt/parse (bad sync byte)'
                        continue
                    # _append_audio needs a real PTS to index this segment
                    # into the continuous buffer at all - without this
                    # check here, a segment that decrypts fine but yields no
                    # PTS-carrying packet (the same transient CDN oddity the
                    # other checks above already retry for) would be
                    # accepted as "fetched" by this loop and permanently
                    # marked done below, even though _append_audio goes on
                    # to silently drop it - no later poll would ever retry
                    # it, leaving a permanent gap in that epoch's audio
                    # coverage for this PTS range instead of a transient one.
                    if not _scan_ts_index(adata)[0]:
                        fail_reason = 'no PTS found in decrypted segment'
                        continue
                    # No per-segment print here on the *first-attempt*
                    # success path: this runs on every fetched audio segment
                    # across _AUDIO_FETCH_WORKERS concurrent workers for
                    # every open channel, and each logger.debug() serializes
                    # on enigma2's global debug-log mutex plus a blocking
                    # write(2) — frequent enough at that volume to stall the
                    # reactor's main thread. _slice_audio_for_window already
                    # logs a match when a window is actually consumed. A
                    # retry succeeding is rare enough (only when attempt 1
                    # already failed) that logging it doesn't have the same
                    # volume problem, and it's the only way to ever confirm
                    # this retry is actually saving segments rather than
                    # every session just happening not to need it.
                    if _attempt > 0:
                        logger.debug('%s/%s: Event: audio segment recovered on retry'
                                     ' (attempt 1 failed: %s)', channel_id, seq, fail_reason)
                    results[seq] = adata
                    return
                except Exception as exc:
                    # This runs via pool.submit() below, whose Future is
                    # never awaited or .result()-ed - any exception raised
                    # in here would otherwise vanish with zero trace (no
                    # log, no traceback), indistinguishable from the CDN
                    # genuinely lacking the segment. Caught here so it's at
                    # least visible and gets the same retry as every other
                    # failure shape above.
                    fail_reason = f'{type(exc).__name__}: {exc}'
                    continue
            logger.debug('%s/%s: Event: audio segment unavailable after retry (%s)'
                         ' | Action: drop this segment, next poll will retry',
                         channel_id, seq, fail_reason)

        workers = min(_AUDIO_FETCH_WORKERS, len(to_fetch))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for seq, url, k, iv, _disc in to_fetch:
                pool.submit(_fetch_one, seq, url, k, iv)
        # The `with` block above only returns once every submitted fetch has
        # completed (ThreadPoolExecutor.__exit__ waits) - only now is it
        # safe to append results to the buffer, strictly in seq order
        # regardless of which worker finished first. See _append_audio's
        # own docstring for why order matters here in a way it never did
        # for the old flat, order-independent audio ring.
        for seq, _url, _k, _iv, disc in to_fetch:
            adata = results.get(seq)
            if adata is None:
                continue
            _append_audio(state, channel_id, seq, disc, adata)
            fetched_seqs.add(seq)

        if state._audio_prefetch_stop.wait(timeout=state.seg_duration / 5):
            break


# ── Video pre-fetch + mux thread ─────────────────────────────────────────────

def _video_prefetch_loop(channel_id: str, state: '_ChannelState',
                         variant_idx: int):
    """Background thread: pre-fetch video segments into a continuous
    per-epoch buffer, and cut+mux fixed-duration output windows from it as
    soon as each window is fully buffered.

    Polls the video variant playlist, fetches each new segment (decrypting
    as needed), appends its bytes to state._video_buf (flushing any partial
    window and opening a fresh buffer at every real CDN discontinuity), and
    drains _cut_and_mux_next_window in a loop after each append - which
    slices whatever audio buffer covers the same PTS range (see
    _slice_audio_for_window) and stores the muxed result in
    state._ready_segs, keyed by an internal chunk_seq rather than the CDN's
    own media-sequence number. _handle_segment then serves from that cache
    with no blocking wait in the steady-state case.

    Combined with _handle_playlist's own readiness check (withholding a
    chunk_seq from the served playlist until this loop has actually
    produced it), the muxed window is normally ready well before Enigma2 is
    allowed to request it. At a real content transition where the matching
    audio hasn't been buffered yet, _resolve_late_audio_window's extended
    retry and _handle_segment's own deadline are what give that case a real
    chance to land in time too.

    variant_idx is re-resolved against state.variant_urls on every poll so
    that CDN URL rotations (signed-URL token refresh) are picked up without
    restarting the thread.
    """
    is_vod = channel_id.startswith('vod')
    # VOD only: the most recently fetched segment, held back and not yet
    # muxed - see the is_vod branch below for why. dict keys: vdata, v_pts,
    # all_pts, is_disc, pending_disc_ahead. None when nothing is
    # currently pending (channel start, or right after a flush).
    vod_pending_video: 'dict | None' = None
    fetched_seqs: set = set()
    idle_log_at = 0.0   # monotonic time of the last idle-poll diagnostic line
    # Seqs whose disc-boundary side effects (video_epoch bump, buffer
    # rollover) have already been applied - distinct from fetched_seqs,
    # which only records a *successful* append. A segment whose fetch fails
    # after its is_disc bump (network blip, decrypt failure, ...) stays out
    # of fetched_seqs and is retried on the next poll; without this
    # separate set, that retry would see the same still-in-window
    # #EXT-X-DISCONTINUITY tag and bump video_epoch a second time for one
    # real transition, permanently desyncing it from audio_epoch.
    bumped_disc_seqs: set = set()
    # Seeded from state rather than hardcoded 0, and the buffer/cursor are
    # always freshly (re)created here rather than reused across a thread
    # respawn (see _handle_playlist's is_alive() check): the audio side's
    # own counter - immune to this thread's restarts - has kept advancing
    # the whole time this thread was down, so resuming at a stale local
    # buffer would risk appending post-restart bytes at the wrong PTS
    # position. See current_video_epoch().
    video_epoch = state.current_video_epoch()
    with state._lock:
        # is_disc_epoch=True (revised - was False): a freshly (re)started
        # thread's buffer has no reliable continuity with whatever came
        # before it, on *any* of the axes that matter here, not just CC:
        #  - CC baseline: state._cc_state reflects wherever the *previous*
        #    thread instance (or nothing, on a plain first start) left off,
        #    unrelated to this fresh buffer - see _cc_resync_pending below.
        #  - Real PTS: on a respawn (crash, or a deliberate close/reopen
        #    after being away - e.g. leaving live to watch a recording and
        #    returning), real time kept moving the whole time this thread
        #    was down; whatever real content is airing *now* is a genuine
        #    jump from wherever the old instance left off, PCR/PTS-wise,
        #    even though editorially nothing "transitioned".
        #  - Audio alignment: confirmed in production - reconnecting after
        #    being away long enough for the CDN to move on, video's and
        #    audio's own renditions can each be independently mid-transition
        #    at the moment of reconnect (one already past a stitch boundary
        #    the other hasn't reached yet), so this buffer's very first
        #    window can legitimately need the same fast-fail/retry handling
        #    as a real #EXT-X-DISCONTINUITY (is_disc_window's short peek
        #    wait + background retry, not the ordinary ~8s synchronous
        #    wait) - without it, this window (and the epoch-resync it
        #    depends on to recover) was stuck for a good ~40s of broken/
        #    silent audio pairing before self-correcting.
        # A previous version of this comment argued CC-only, not is_disc,
        # specifically to avoid a false is_disc flag - but the PCR/PTS
        # really did just jump on every path that reaches here, the same
        # physical reality _cc_resync_pending below already exists to
        # correct for; is_disc=True here is accurate, not false.
        state._video_buf = _RenditionBuffer(video_epoch, is_disc_epoch=True)
        state._video_cursor_pts = None
        # Belt-and-suspenders with is_disc_epoch=True above (is_disc=True
        # already forces reset_cc=True in _cut_and_mux_next_window) - kept
        # so this path still gets a clean CC baseline even if is_disc_epoch
        # is ever narrowed again in the future.
        state._cc_resync_pending = True

    while True:
        if state._video_prefetch_stop.is_set():
            break

        # Re-read the variant URL on every iteration so CDN URL rotations
        # (token refresh) are picked up without restarting the thread.
        urls = state.variant_urls          # atomic reference read (GIL)
        if variant_idx >= len(urls):
            if state._video_prefetch_stop.wait(timeout=state.seg_duration / 5):
                break
            continue
        variant_url = urls[variant_idx]

        first_playlist, state._video_first_playlist = state._video_first_playlist, None
        vtext = first_playlist[1] if first_playlist and first_playlist[0] == variant_url else None
        if not vtext:
            vtext = _fetch(variant_url, tag='video-prefetch:playlist', headers=state.headers)
        if not vtext:
            if state._video_prefetch_stop.wait(timeout=state.seg_duration / 5):
                break
            continue

        # Diagnostic only - see HLSPlaylist.ad_markers's docstring. Logged
        # unconditionally when present (not deduped across polls) since
        # this is meant to be pulled once a real capture confirms whether
        # a provider sends any of these at all, not left running long-term.
        ad_markers = HLSPlaylist.ad_markers(vtext)
        if ad_markers:
            logger.debug('%s: video playlist ad markers: %s', channel_id, ad_markers)

        vbase = variant_url.split('?')[0].rsplit('/', 1)[0] + '/'
        try:
            vseq, vsegs = HLSPlaylist.segments(vtext, vbase)
            all_key_urls, all_ivs, all_discs = HLSPlaylist.segment_keys(vtext, vbase)
        except Exception:
            if state._video_prefetch_stop.wait(timeout=state.seg_duration / 5):
                break
            continue

        # Anchor the pacing throttle's floor to this playlist's own reported
        # position, not just to real Enigma2 requests. _last_requested_seq
        # defaults to 0, but segment sequence numbers are arbitrary - a live
        # channel's media sequence is cumulative for as long as it's been
        # running (already in the thousands after a few hours), so on a
        # fresh zap the throttle below would otherwise block this loop
        # until Enigma2's first real request happened to land and bump the
        # baseline up from 0. Seeding here means the floor already tracks
        # the live edge by the time this loop reaches its own per-segment
        # throttle check, making it a true no-op for live (this loop's own
        # `seq` is always <= vseq + a small window). For VOD, whose
        # reported sequence is static, this just seeds the correct starting
        # floor once; real _handle_segment calls take over from there.
        state.record_segment_request(max(0, vseq - 1))

        # Trim fetched_seqs to avoid unbounded growth on long-running sessions.
        # Same window as _append_audio's own dedup sets - see _SEQ_REPROCESS_WINDOW.
        fetched_seqs = {s for s in fetched_seqs if s >= vseq - _SEQ_REPROCESS_WINDOW}
        bumped_disc_seqs = {s for s in bumped_disc_seqs if s >= vseq - _SEQ_REPROCESS_WINDOW}

        _fill_key = all_key_urls[-1] if all_key_urls else None
        _fill_iv = all_ivs[-1] if all_ivs else None
        while len(all_key_urls) < len(vsegs):
            all_key_urls.append(_fill_key)
            all_ivs.append(_fill_iv)
        while len(all_discs) < len(vsegs):
            all_discs.append(False)

        new_seqs = 0
        for i, url in enumerate(vsegs):
            seq = vseq + i
            if seq < 0:
                # CDN sent a bogus/negative #EXT-X-MEDIA-SEQUENCE for this
                # poll (observed in production). seq feeds AES128.iv()'s
                # seq.to_bytes(16, 'big'), which raises OverflowError on a
                # negative value - uncaught, that kills this whole thread.
                # Skip the segment and let the next poll (fresh vseq) recover
                # rather than crashing the loop and orphaning the video
                # buffer for segments already appended this poll.
                logger.debug('%s/%s: Event: negative sequence number'
                             ' from CDN playlist | Action: skip segment, wait for next poll',
                             channel_id, seq)
                continue
            if seq in fetched_seqs:
                continue
            if state._video_prefetch_stop.is_set():
                break

            # Pace fetching to stay within _PREFETCH_LEAD_SEGMENTS of what
            # Enigma2 has actually requested (record_segment_request)
            # instead of racing ahead as fast as network+CPU allow. A
            # no-op for live (segments never arrive faster than the live
            # rate to begin with); for VOD, whose entire playlist is
            # visible from the first poll, this is what keeps this loop
            # from consuming the whole asset in one burst - observed in
            # production climbing to 150+ segments ahead of actual
            # playback, which starves the audio side of near-term content
            # in favor of far-future entries long before video needs them.
            #
            # Logged when it actually engages: this should be a no-op for
            # live, so a log line here means either a VOD asset legitimately
            # pacing itself, or - if it doesn't resolve quickly - Enigma2's
            # own request cadence has stalled for some unrelated reason.
            pace_wait_start = None
            while seq > state.last_requested_seq() + _PREFETCH_LEAD_SEGMENTS:
                if pace_wait_start is None:
                    pace_wait_start = time.monotonic()
                    logger.debug('%s/%s: pacing - last requested seq is %s,'
                                 ' waiting for it to reach %s before fetching further ahead',
                                 channel_id, seq, state.last_requested_seq(),
                                 seq - _PREFETCH_LEAD_SEGMENTS)
                if state._video_prefetch_stop.wait(timeout=0.5):
                    break
            if pace_wait_start is not None:
                logger.debug('%s/%s: pacing wait ended after %.1fs'
                             ' (last requested seq now %s)',
                             channel_id, seq, time.monotonic() - pace_wait_start,
                             state.last_requested_seq())
            if state._video_prefetch_stop.is_set():
                break

            is_disc = all_discs[i]
            # VOD only - see _DISC_LOOKAHEAD_SEGMENTS's own comment. A pure
            # list lookup over data this poll already fetched in full, not a
            # wait: safe to compute unconditionally whenever is_vod, cheap
            # enough to redo every segment.
            pending_disc_ahead = is_vod and any(
                all_discs[i + 1:i + 1 + _DISC_LOOKAHEAD_SEGMENTS])
            if is_disc and seq not in bumped_disc_seqs:
                bumped_disc_seqs.add(seq)
                if is_vod and vod_pending_video is not None:
                    # The upcoming segment starts a genuinely new epoch, so
                    # there is no real "next same-epoch segment" left to
                    # give the outgoing epoch's still-open segment an exact
                    # end boundary - using the new segment's own v_pts
                    # instead would incorrectly bridge this window across a
                    # real content transition. Flush it now with the
                    # estimate instead, using the *current* (pre-bump)
                    # video_epoch it actually belongs to.
                    # pylint: disable=unsubscriptable-object
                    # (false positive: astroid doesn't narrow a string-quoted
                    # 'dict | None' annotation across an "is not None" block
                    # guard - already checked two lines above)
                    _mux_whole_video_segment(
                        channel_id, state, video_epoch,
                        vod_pending_video['vdata'], vod_pending_video['v_pts'],
                        _estimate_v_end_pts(vod_pending_video['all_pts']),
                        vod_pending_video['is_disc'], vod_pending_video['pending_disc_ahead'])
                    # pylint: enable=unsubscriptable-object
                    vod_pending_video = None
                    video_epoch = state.current_video_epoch()
                video_epoch += 1
                state.set_video_epoch(video_epoch)
                logger.debug('%s/%s: Event: CDN discontinuity tag | Action:'
                             ' flush partial window, video_epoch -> %s (audio_epoch=%s)',
                             channel_id, seq, video_epoch, state.current_audio_epoch())
                # Normally a no-op by the time we get here (every append is
                # already fully cut into its own window immediately below),
                # kept as a defensive flush in case anything was ever left
                # pending.
                while _cut_and_mux_next_window(channel_id, state):
                    pass
                # Pick up any resync _cut_and_mux_next_window may have just
                # applied while flushing (see _MAX_AUDIO_EPOCH_LAG) - it
                # updates state._video_epoch directly, which this loop's own
                # local video_epoch variable wouldn't otherwise reflect,
                # leaving the is_disc_epoch=True buffer created just below
                # tagged with a stale epoch number inconsistent with it.
                video_epoch = state.current_video_epoch()
                state._video_buf.mark_closed()
                state._video_buf = _RenditionBuffer(video_epoch, is_disc_epoch=True)
                state._video_cursor_pts = None

            preload = state._video_preload.pop(url, None)
            vdata = preload.result() if preload is not None else None   # started at the first playlist request
            if not vdata:
                vdata = _fetch(url, binary=True, timeout=state.seg_duration * 0.8,
                               tag='video-prefetch:segment', headers=state.headers)
            if not vdata:
                continue

            key_url = all_key_urls[i] if i < len(all_key_urls) else None
            iv_explicit = all_ivs[i] if i < len(all_ivs) else None
            if key_url:
                kb = AES128.fetch_key(key_url, _fetcher(state))
                if kb:
                    vdata = AES128.decrypt(vdata, kb,
                                           AES128.iv(iv_explicit, seq))
            if not vdata or vdata[0] != 0x47:
                continue

            pts_offsets, pat_pmt = _scan_ts_index(vdata)
            if not pts_offsets:
                # No PTS-carrying packet found at all (garbled/unusual
                # framing) - can't be indexed into the continuous buffer,
                # cut() has nothing to bisect against. Rather than losing
                # this slice of the stream outright (dropping it here is a
                # real content gap/freeze, not a graceful degrade), mux and
                # publish it as its own standalone window immediately -
                # matching the old per-segment design, which still served a
                # segment like this (with injected silence audio, since
                # Enigma2 permanently drops audio on a true video-only
                # segment - see _ffmpeg_mux's own warning). Approximates
                # this window's PTS/duration from the buffer's last known
                # position purely for playlist/mux purposes - never touches
                # state._video_buf/_video_cursor_pts, since this segment was
                # never actually indexed into that buffer.
                logger.debug('%s/%s: Event: no PTS found in video segment'
                             ' | Action: mux standalone (approx PTS), skip buffer',
                             channel_id, seq)
                if is_vod:
                    # VOD's whole-segment path (see _mux_whole_video_segment)
                    # never populates state._video_buf/_video_cursor_pts, so
                    # this fallback needs its own last-known-position - the
                    # still-open pending segment (if any) serves double duty.
                    approx_start = vod_pending_video['v_pts'] if vod_pending_video else None
                else:
                    approx_start = state._video_cursor_pts
                    if approx_start is None and state._video_buf.pts_list:
                        approx_start = state._video_buf.pts_list[-1]
                if approx_start is None:
                    approx_start = 0
                approx_end = approx_start + int(state.seg_duration * 90000)
                muxed = _ffmpeg_mux(vdata, None, state, is_disc=False, track_cc=True,
                                    v_pts=approx_start)
                with state._lock:
                    chunk_seq = state._next_chunk_seq
                    state._next_chunk_seq += 1
                    state._ready_segs[chunk_seq] = muxed
                    state._window_meta[chunk_seq] = _WindowMeta(
                        video_epoch, approx_start, approx_end, False)
                    state.assign_live_delta(chunk_seq, approx_start, approx_end, False, approximate=True)
                    while len(state._ready_segs) > _MAX_SEG_CACHE:
                        k, _ = state._ready_segs.popitem(last=False)
                        state._window_meta.pop(k, None)
                fetched_seqs.add(seq)
                continue

            if is_vod:
                # See _mux_whole_video_segment's own docstring for why VOD
                # takes this path instead of state._video_buf/cut() for
                # video's own boundaries, and for why both ends of a
                # segment's true PTS span (not an approximated one) matter -
                # min()/max(), not pts_offsets[0][1]/a nominal duration,
                # independent of B-frame transmission-order reordering.
                #
                # v_end_pts is never estimated here anymore. A previous
                # version called what is now _estimate_v_end_pts on every
                # segment - confirmed, by directly comparing consecutive
                # real windows' own logged PTS ranges in a production
                # session, to systematically overshoot the next segment's
                # true start by ~40-55ms on *every* boundary, no exceptions:
                # the frame-interval extrapolation is only ever a guess,
                # and this segment's actual successor (fetched next, on a
                # later iteration of this same loop) always knows the real
                # answer already. So muxing is deferred one segment behind
                # instead: each segment is held in vod_pending_video until
                # the *next* segment's own real min(pts) gives its exact end
                # boundary - exactly mirroring how live's cut(cursor, None)
                # never estimates a boundary either, just uses whatever is
                # actually, already there. _estimate_v_end_pts's fallback is
                # used only where no successor will ever exist to ask
                # instead - the final segment of the whole asset, or one
                # immediately preceding a real disc (handled above, at the
                # is_disc branch).
                all_pts = [p for _, p in pts_offsets]
                v_pts = min(all_pts)
                # False positive - see the flush call above for why.
                # pylint: disable-next=unsubscriptable-object
                if vod_pending_video is not None and v_pts == vod_pending_video['v_pts']:
                    # CDN published a duplicate video segment (same PTS as
                    # the still-open pending one) - common at content-
                    # transition boundaries where the encoder repeats the
                    # final frame. Simply not muxing it produces no extra
                    # output window at all, same as live's equivalent check
                    # just below.
                    logger.debug('%s/%s: Event: duplicate video PTS (CDN frame-repeat'
                                 ' at splice) v_pts=%s | Action: drop, no window muxed',
                                 channel_id, seq, v_pts)
                    fetched_seqs.add(seq)
                    continue
                if vod_pending_video is not None:
                    _mux_whole_video_segment(
                        channel_id, state, video_epoch,
                        vod_pending_video['vdata'], vod_pending_video['v_pts'], v_pts,
                        vod_pending_video['is_disc'], vod_pending_video['pending_disc_ahead'])
                    # Pick up any epoch resync _mux_whole_video_segment may
                    # have just applied (see _MAX_AUDIO_EPOCH_LAG) - same
                    # reasoning as live's equivalent call below.
                    video_epoch = state.current_video_epoch()
                vod_pending_video = {
                    'vdata': vdata, 'v_pts': v_pts, 'all_pts': all_pts,
                    'is_disc': is_disc, 'pending_disc_ahead': pending_disc_ahead,
                }
                fetched_seqs.add(seq)
                new_seqs += 1
                continue

            v_pts = pts_offsets[0][1]

            vbuf = state._video_buf
            if vbuf.pts_list and v_pts == vbuf.pts_list[-1]:
                # CDN published a duplicate video segment (same PTS as the
                # previous one) - common at content-transition boundaries
                # where the encoder repeats the final frame. Simply not
                # appending it produces no extra output window at all,
                # replacing the old design's mark_duplicate/is_duplicate
                # playlist-omission bookkeeping.
                logger.debug('%s/%s: Event: duplicate video PTS (CDN frame-repeat'
                             ' at splice) v_pts=%s | Action: drop, not appended to buffer',
                             channel_id, seq, v_pts)
                fetched_seqs.add(seq)
                continue

            vbuf.append(vdata, pts_offsets, pat_pmt)
            if state._video_cursor_pts is None:
                state._video_cursor_pts = v_pts

            fetched_seqs.add(seq)
            new_seqs += 1

            # Cut this CDN segment out as its own output window immediately -
            # see _cut_and_mux_next_window's docstring for why windows are
            # sized to CDN segment boundaries (keyframe/SPS-PPS alignment),
            # not a fixed target duration.
            _cut_and_mux_next_window(channel_id, state)
            # Pick up any epoch resync _cut_and_mux_next_window may have
            # just applied (see _MAX_AUDIO_EPOCH_LAG) - state._video_epoch
            # is updated directly there, which this loop's own local
            # video_epoch variable wouldn't otherwise reflect, leaving the
            # *next* real disc tag's video_epoch += 1 bumping from a stale
            # baseline.
            video_epoch = state.current_video_epoch()

        if is_vod and vod_pending_video is not None and '#EXT-X-ENDLIST' in vtext:
            # This is genuinely the last segment of the whole VOD asset -
            # no further segment will ever arrive to give it a real end
            # boundary, so _estimate_v_end_pts's fallback is the correct
            # choice here (there's no "next real segment" case downstream
            # of this that it could ever wrongly shortcut ahead of).
            _mux_whole_video_segment(
                channel_id, state, video_epoch,
                vod_pending_video['vdata'], vod_pending_video['v_pts'],
                _estimate_v_end_pts(vod_pending_video['all_pts']),
                vod_pending_video['is_disc'], vod_pending_video['pending_disc_ahead'])
            vod_pending_video = None
            video_epoch = state.current_video_epoch()

        if (is_vod and vod_pending_video is None and '#EXT-X-ENDLIST' in vtext
                and vsegs and all(vseq + i in fetched_seqs for i in range(len(vsegs)))):
            if not state._vod_complete:
                logger.debug('%s: Event: whole VOD asset fetched and muxed (segments %s-%s)'
                             ' | Action: advertise #EXT-X-ENDLIST', channel_id, vseq,
                             vseq + len(vsegs) - 1)
            state._vod_complete = True

        if state._video_prefetch_stop.is_set():
            break
        # Skip the poll-interval sleep when new segments were just processed.
        # Re-polling immediately ensures the next segment is picked up as soon
        # as it appears on the CDN, eliminating the up-to-1 s gap that would
        # otherwise push the mux completion past the handler's deadline.
        if new_seqs == 0:
            # Diagnostic: a poll that finds nothing new is normal for a
            # moment, but if it persists (VOD stalling at an ad transition)
            # this shows what the CDN playlist actually contains.
            now = time.monotonic()
            if now - idle_log_at >= 10.0:
                idle_log_at = now
                logger.debug('%s: video poll idle: vseq=%s segments=%s last_seq=%s endlist=%s'
                             ' pending_vod_segment=%s fetched_max=%s last_requested_seq=%s',
                             channel_id, vseq, len(vsegs), vseq + len(vsegs) - 1,
                             '#EXT-X-ENDLIST' in vtext, vod_pending_video is not None,
                             max(fetched_seqs) if fetched_seqs else None,
                             state.last_requested_seq())
            if state._video_prefetch_stop.wait(timeout=state.seg_duration / 5):
                break


def active_channel_url(channel_id: str) -> 'str | None':
    """Return the local proxy master-playlist URL for *channel_id* if it
    already has a *running* session registered, or None otherwise.

    Lets a provider about to mint a fresh stream URL check first whether the channel is
    already running - under some other pool slot's session - so it can
    reuse that instead of silently repointing an already-live channel at
    a different device identity's session (see buildStreamURL's
    docstring for why that corrupts recordings of an already-live
    channel).

    Checks _audio_prefetch_stop rather than just presence in _channels:
    close_channel() deliberately leaves the state entry in place while
    its threads wind down (see its docstring), so a channel that was
    just zapped away from would otherwise look "active" here too and get
    handed a URL with nothing left running behind it - register_channel()
    is what actually knows how to undo that close and needs to run for
    this channel_id in that case, not be skipped.
    """
    state = _get_state(channel_id)
    if state is None or state._audio_prefetch_stop.is_set():
        return None
    return f'http://{PROXY_HOST}:{PROXY_PORT}/auto/{channel_id}.m3u8'


def _session_id(master_url: str, param: str = 'sid') -> 'str | None':
    """Extract the id of the stream session (the query parameter *param*, ?sid=... by default) from a master URL.

    Identifies which real session of the stream server a master_url belongs to, as opposed to which token/device
    identity it was minted with (those rotate independently of the session - see register_channel's use of it).
    Returns None if absent, so a URL that never carries one is never taken for a session that "changed".
    """
    return (parse_qs(urlparse(master_url).query).get(param) or [None])[0]


def register_channel(channel_id: str, real_master_url: str, url_refresher=None, headers=None,
                     refresh_on_failure: bool = False, session_param: str = 'sid') -> str:
    """Register a channel and return the local proxy URL for its master playlist.

    *url_refresher*, when supplied, is stored on the channel state and called
    from the handler thread (safe to block) when the master playlist fetch fails
    with an empty JWT.  It must return a fresh master URL with a valid token, or
    None on failure.

    *real_master_url* may be empty when the provider only knows the stream address after a network call: the
    *url_refresher* then resolves it, once, on the first worker thread that needs it (see resolved_master_url).
    A later registration with an empty address leaves the address the channel already has.

    *headers* (dict, or a callable returning one: it runs on the worker thread that needs the headers, so it may
    wait for the network) are sent with every request the proxy makes for this channel; see header_suffix for what
    Enigma2 itself needs to send.

    *refresh_on_failure*: also call the *url_refresher* when the master playlist can't be fetched, not just for
    an empty JWT (an address that expires, a redirect that ends up somewhere else).

    *session_param*: the query parameter of the master address that carries the id of the stream session ('sid').
    A registration of a running channel whose address has another session id starts the channel's served data
    afresh, as the new session numbers its segments from a different base. A provider whose addresses carry no
    session id is not affected.

    Idempotent for an already-registered channel_id: reuses its existing
    state (pre-fetch threads, audio ring, muxed-segment cache) and just
    refreshes master_url, rather than tearing it down and starting from
    scratch. This is what makes it safe for *any* caller to invoke freely
    for the same channel_id - live playback, a recording of the channel
    currently being watched, or a recording timer simply re-resolving the
    same service mid-recording - without disrupting an already-running
    stream. The previous version always replaced same-channel_id state on
    the theory that this only happens on a deliberate re-tune; in
    production a single recording's service reference got re-resolved a
    dozen times in 10 minutes with no live viewer involved at all, each
    call restarting the channel from scratch (fresh master/variant fetch,
    empty audio ring, epoch reset to 0) and audibly corrupting the
    recording. There is no actual caller that depends on a same-channel_id
    call forcing a restart - that's what close_channel() is for, explicitly.

    Different channel_ids never evict each other through this call, so
    independent concurrent channels (e.g. one being recorded while another
    is watched live) each keep their own pre-fetch threads undisturbed.
    Closing a channel nobody asked to replace is handled elsewhere: callers
    that track "the live slot" call close_channel() explicitly the moment
    they know it moved on (see StreamProxy.playServiceExtension), and the
    idle reaper started alongside the HTTP server in start() is the backstop
    for anything that stops being read without ever calling back in here
    (e.g. a recording that simply ends).
    """
    with _state_lock:
        existing = _channels.get(channel_id)
        if existing is not None:
            existing.session_param = session_param
            old_sid = _session_id(existing.master_url, session_param)
            new_sid = _session_id(real_master_url, session_param)
            if real_master_url:
                existing.master_url = real_master_url
            if url_refresher is not None:
                existing.url_refresher = url_refresher
                existing.refresh_on_failure = refresh_on_failure
            if headers is not None:
                existing.headers = headers
            # A new stream session (fresh session id) starts its own,
            # unrelated media-sequence numbering from a low base - see
            # active_channel_url for when this call gets a genuinely
            # new session (active_channel_url returned None: the previous
            # session's threads had actually stopped) versus the same
            # session simply being re-resolved (session id unchanged, e.g. a
            # recording re-resolving its sref) that this whole idempotent-
            # reuse path exists to protect. This clears the *served* cache
            # only (_ready_segs/_window_meta/_provisional_segs) - not
            # audio_epoch/video_epoch, which already correctly carry
            # forward as a real, monotonic count of transitions this
            # channel_id has seen, session boundary or not, and not
            # _video_buf/_audio_bufs (bytes actively owned by the running
            # prefetch threads - clearing them from here, off those
            # threads, would race a concurrent append/cut and risk a null
            # dereference; any PTS discontinuity a new session's segments
            # introduce into an old buffer surfaces at worst as one
            # mis-sized window, not a wrong-content serve, since chunk_seq
            # - unlike the old CDN-seq-keyed cache - never reuses a value
            # for unrelated content in the first place). Originally
            # (before chunk_seq existed) a low CDN seq the new session was
            # about to reuse could collide with a still-cached entry from
            # whenever the old session last used that same low number -
            # confirmed in production as _handle_segment's fast path
            # serving several seconds of stale, wrong-content bytes
            # immediately on re-zap; chunk_seq's own monotonic,
            # never-reused numbering (see _next_chunk_seq) makes that
            # specific collision structurally impossible now, but the
            # served-cache clear is kept regardless so a re-zap doesn't
            # briefly serve leftover *content* from the just-abandoned
            # session while the new one spins up.
            # Checked *before* clearing the stop Events just below, so this
            # reflects whether the pipeline had actually died (close_channel()
            # ran to completion) since this channel was last used - not just
            # whether the session token changed. A same-session re-zap (session
            # id unchanged) still tore down and is restarting the prefetch
            # threads from scratch here, which will resume fetching from
            # whatever the CDN's live edge is *now* - real time kept moving
            # the whole time this channel sat unwatched, so the entries
            # _ready_segs already holds are frames from before the stop,
            # already behind that live edge. Without also clearing on
            # was_closed (not just a sid change), _handle_playlist would
            # advertise - and Enigma2 would play - several seconds of that
            # stale rerun before the restarted threads caught up and started
            # producing genuinely current chunk_seqs: observed in production
            # returning from a brief recording playback to live TV, where
            # the return glitched/looked frozen because it silently
            # resumed ~70s in the past instead of at the live edge.
            was_closed = existing._audio_prefetch_stop.is_set()
            if was_closed or (new_sid is not None and new_sid != old_sid):
                existing.discard_stale_stream()
            # Undo any prior close_channel(): its stop Events are one-shot
            # (never cleared elsewhere) and would otherwise permanently kill
            # every prefetch thread started for this reused state, even
            # though the caller is asking for the channel to run again.
            existing._audio_prefetch_stop.clear()
            existing._video_prefetch_stop.clear()
            if was_closed:
                # The prefetch threads actually died (close_channel() ran to
                # completion) since this channel was last used, so whatever
                # the audio ring held is stale by the time new threads spin
                # back up. Re-arm the one-shot pre-population pass so the
                # next playlist request gives the ring a fresh head start
                # again, same as a brand-new channel gets. Without this, a
                # zap-away-and-back within the idle-reap window skips
                # pre-population (already run once, ever, per this flag) and
                # the video prefetch loop's first segment(s) find nothing in
                # the ring to pair with, burning the full audio-wait timeout
                # before falling back to silence - observed as an extra ~8s
                # added to the zap. Left alone (was_closed False) for the
                # case this function exists to protect: a recording
                # re-resolving its service reference repeatedly while still
                # running must not disrupt an already-fine audio ring.
                existing._audio_prepopulated = False
        else:
            state = _ChannelState(real_master_url)
            state.url_refresher = url_refresher
            state.refresh_on_failure = refresh_on_failure
            state.session_param = session_param
            state.headers = headers
            _channels[channel_id] = state
    return f'http://{PROXY_HOST}:{PROXY_PORT}/auto/{channel_id}.m3u8'


def close_channel(channel_id: str) -> bool:
    """Explicitly close one channel's pre-fetch threads without touching any
    other channel.  Used when a caller knows a specific channel is no longer
    needed (e.g. the live slot zapped to a different service) and wants that
    cleaned up immediately rather than waiting for the idle reaper.

    Returns False if the channel wasn't registered. As with register_channel's
    old-entry teardown, close() runs off-thread and the entry is left in
    _channels so an in-flight request still finds cached state instead of a
    404 while it winds down.
    """
    from . import LiveProxyMuxed  # deferred: LiveProxyMuxed imports from this module
    muxed_closed = LiveProxyMuxed.close_channel(channel_id)
    with _state_lock:
        state = _channels.get(channel_id)
    if state is None:
        return muxed_closed
    if channel_id.startswith('vod'):
        # a VOD channel is never read again once its player has left, so free its (large) buffers at once
        # instead of holding them until the idle reaper removes the entry
        threading.Thread(target=state.close_and_release, args=(channel_id,), daemon=True).start()
    else:
        threading.Thread(target=state.close, daemon=True).start()
    return True


_IDLE_TIMEOUT = 60.0   # seconds of no HTTP request before an unreferenced channel's
#                      # pre-fetch threads (and CDN polling / ffmpeg muxing) are torn
#                      # down; the backstop for any teardown path that doesn't call
#                      # close_channel itself (e.g. a recording simply ending, or a
#                      # zap that for some reason missed the explicit live-slot close)
_REAP_INTERVAL = 15.0  # how often the reaper scans for idle channels


def _reap_idle_channels():
    while True:
        time.sleep(_REAP_INTERVAL)
        now = time.monotonic()
        with _state_lock:
            idle_ids = [cid for cid, st in _channels.items()
                        if now - st.last_access > _IDLE_TIMEOUT]
        for cid in idle_ids:
            # Re-check right before acting: a request landing between the
            # scan above and now would otherwise tear down a channel that's
            # not actually idle anymore.
            with _state_lock:
                st = _channels.get(cid)
                if st is None or time.monotonic() - st.last_access <= _IDLE_TIMEOUT:
                    continue
            logger.debug('%s: idle %.0fs+, reaping', cid, _IDLE_TIMEOUT)
            st.close()
            with _state_lock:
                if _channels.get(cid) is st:
                    _channels.pop(cid, None)


# ── HTTP proxy handler ────────────────────────────────────────────────────────

class _ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


class _FetchedText(str):
    """The text of a playlist, with the address it was fetched from in .url: after the redirects of the request.

    Relative addresses in a playlist are relative to that one, not to the address that was asked for: the master
    playlist of a channel can be behind a redirect to another host (a playout host that redirects to
    an ad-insertion service), and its variants are then found there and nowhere on the host of the redirecting address."""
    url = ''


def _base_url(text, requested_url: str) -> str:
    """The directory address relative addresses in the playlist *text* are relative to (the query string is no
    part of it): where it was finally fetched from, *requested_url* if that isn't known."""
    return (getattr(text, 'url', '') or requested_url).split('?')[0].rsplit('/', 1)[0] + '/'


def _background(func, *args, **kwargs) -> Future:
    """Run func(*args, **kwargs) on a thread of its own; the Future has its result."""
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='proxy-bg')
    future = pool.submit(func, *args, **kwargs)
    pool.shutdown(wait=False)   # the worker ends with the call
    return future


def _fetch(url: str, binary: bool = False, timeout: int = 6, tag: str = '', headers: 'dict | None' = None):
    """Fetch *url* and return text or bytes; returns None on any error.

    Default timeout is kept close to _SEG_DURATION rather than requests'
    usual generous allowance: this covers playlist fetches (master,
    variant, audio) that run synchronously inside an HTTP handler thread
    while eServiceMP3 is waiting on the response - including, on the
    /auto/{channel}.m3u8 path, during the pipeline's own initial NULL ->
    READY setup for a just-zapped channel. A slow CDN response there should
    fail fast rather than let that handler thread - and whatever in the
    playback pipeline is blocked on it - sit for up to the old 15s.

    *headers* are extra request headers of the channel (the provider's, see register_channel).

    *tag* identifies the call site in the error log only (e.g.
    'handler:playlist' vs 'video-prefetch') - several different call sites
    (an HTTP handler thread serving Enigma2 directly, and a background
    pre-fetch thread) fetch the exact same URLs independently, and a bare
    "fetch error" line can't otherwise say which one actually hit a given
    timeout - which matters when diagnosing whether a CDN blip was ever
    exposed to Enigma2 at all or was fully absorbed by a background retry.
    """
    try:
        r = _SESSION.get(url, timeout=timeout, headers=headers)
        r.raise_for_status()
        if binary:
            return r.content
        text = _FetchedText(r.text)
        text.url = r.url
        return text
    except Exception as exc:
        label = f' [{tag}]' if tag else ''
        logger.debug('fetch error%s %r: %s', label, url, exc)
        return None


def _fetcher(state: '_ChannelState'):
    """_fetch bound to the request headers of *state*'s channel, in the shape AES128.fetch_key expects."""
    headers = state.headers
    return lambda url, **kwargs: _fetch(url, headers=headers, **kwargs)


def header_suffix(headers: 'dict | None') -> str:
    """The '#Key=Value&...' suffix of a stream address that makes eServiceMP3 send *headers* with its requests.

    A provider whose stream needs request headers appends it to the proxy address it hands to Enigma2: the proxy
    itself gets the headers by register_channel(headers=...) for what it fetches, but a redirect to the real
    stream (a stream the proxy can't process) is followed by Enigma2 and needs them there as well."""
    if not headers:
        return ''
    return '#' + '&'.join(f'{key}={value}' for key, value in headers.items())


def resolved_master_url(state: '_ChannelState') -> str:
    """The master URL of a channel, resolved if its provider left that for later.

    A provider that has to ask a server for the stream address registers the channel with an empty master URL and a
    url_refresher instead of resolving it while Enigma2 is zapping (the service extensions run on the main thread and
    must not wait for the network). The first worker that needs the address resolves it here, on its own thread."""
    if not state.master_url and state.url_refresher:
        with state._master_lock:
            if not state.master_url:
                try:
                    url = state.url_refresher()
                except Exception as exc:
                    logger.debug('url_refresher failed: %s', exc)
                    url = None
                if url:
                    state.master_url = url
    return state.master_url


def _can_refresh(state: '_ChannelState') -> bool:
    """True if a failed master playlist fetch is worth a call of the url_refresher."""
    return state.url_refresher is not None and (state.refresh_on_failure or _has_empty_jwt(state.master_url))


def _has_empty_jwt(url: str) -> bool:
    """Return True when *url* contains an empty jwt= token (jwt=& or jwt= at end)."""
    return '?jwt=&' in url or url.endswith('?jwt=')


_AUTO_MASTER_RETRIES = 2      # extra attempts after a still-failing master fetch
_AUTO_MASTER_RETRY_DELAY = 1.0  # seconds between those attempts


class HLSProxyHandler(BaseHTTPRequestHandler):
    """Handles three URL patterns:

        /master/{channel_id}.m3u8       – master playlist (modified)
        /pl/{channel_id}/{idx}.m3u8     – variant playlist (merged)
        /seg/{channel_id}/{seq}.ts      – TS segment (merged)
    """

    def log_message(self, format, *args):  # pylint: disable=redefined-builtin  # noqa: A002
        pass   # silence the default per-request log lines

    def do_GET(self):
        path = self.path.split('?')[0]
        parts = path.strip('/').split('/')
        try:
            kind = parts[0] if parts else ''
            if kind == 'master' and len(parts) >= 2:
                ch = parts[1].removesuffix('.m3u8')
                self._handle_master(ch)
            elif kind == 'auto' and len(parts) >= 2:
                ch = parts[1].removesuffix('.m3u8')
                self._handle_auto(ch)
            elif kind == 'pl' and len(parts) >= 3:
                ch = parts[1]
                idx = int(parts[2].removesuffix('.m3u8'))
                self._handle_playlist(ch, idx)
            elif kind == 'seg' and len(parts) >= 3:
                ch = parts[1]
                seq = int(parts[2].removesuffix('.ts'))
                self._handle_segment(ch, seq)
            elif kind == 'rec' and len(parts) >= 2:
                ch = parts[1].removesuffix('.ts')
                self._handle_recording(ch)
            elif kind == 'mpl' and len(parts) >= 2:
                ch = parts[1].removesuffix('.m3u8')
                self._handle_muxed(ch, None)
            elif kind == 'mseg' and len(parts) >= 3:
                self._handle_muxed(parts[1], int(parts[2].removesuffix('.ts')))
            else:
                self._send(404, b'Not found', 'text/plain')
        except BrokenPipeError:
            pass  # client disconnected before response was fully sent – normal on zap
        except Exception as exc:
            logger.debug('handler error: %s', exc)
            try:
                self._send(500, b'Internal error', 'text/plain')
            except OSError:
                pass  # socket already gone

    # ── /auto/{channel_id}.m3u8 ───────────────────────────────────────────

    def _handle_auto(self, channel_id: str):
        """Fetch the real master playlist, detect demuxed audio, and either
        process it through the muxing proxy or redirect servicemp3 directly
        to the real URL when no audio muxing is needed."""
        state = _get_state(channel_id)
        if not state:
            self._send(404, b'Unknown channel', 'text/plain')
            return
        resolved_master_url(state)
        text = _fetch(state.master_url, tag='handler:auto-master', headers=state.headers)
        if not text and _can_refresh(state):
            # Cold-slot miss: JWT was empty when the channel was registered
            # (prewarm hadn't completed yet).  Call the refresher, which runs
            # slot.boot() synchronously on this handler thread (safe to block)
            # to obtain a valid token, then retry the fetch.
            fresh_url = state.url_refresher()
            if fresh_url:
                state.master_url = fresh_url
                text = _fetch(state.master_url, tag='handler:auto-master-refreshed', headers=state.headers)
        for attempt in range(_AUTO_MASTER_RETRIES):
            if text:
                break
            # Covers a transient failure unrelated to the cold-slot/empty-JWT
            # case above (observed in production: a provider's own stitching
            # server returning a bare 500 for one channel, gone on the very next
            # attempt) - _fetch() only reports success/failure, not a status
            # code, so this just retries the same URL rather than branching
            # on the failure reason.
            time.sleep(_AUTO_MASTER_RETRY_DELAY)
            text = _fetch(state.master_url, tag=f'handler:auto-master-retry{attempt + 1}', headers=state.headers)
        if not text:
            self._send(502, b'Bad gateway', 'text/plain')
            return
        # Detect a separate audio rendition (EXT-X-MEDIA TYPE=AUDIO).  Only
        # demuxed streams need the muxing proxy; all others are redirected.
        # (A rendition without URI is the audio of the video segments, see needs_audio_mux.)
        if not HLSPlaylist.needs_audio_mux(text):
            # Plain muxed stream: LiveProxyMuxed keeps its ad breaks (discontinuities) from reaching the
            # decoder, or redirects Enigma2 straight to the real URL if the stream can't be processed.
            from . import LiveProxyMuxed  # deferred: LiveProxyMuxed imports from this module
            LiveProxyMuxed.handle_auto(self, channel_id, state)
            return
        # Demuxed stream – hand off to the muxing handler with pre-fetched text.
        self._handle_master(channel_id, text)

    # ── /master/{channel_id}.m3u8 ──────────────────────────────────────────

    def _handle_master(self, channel_id: str, text: str = None):
        state = _get_state(channel_id)
        if not state:
            self._send(404, b'Unknown channel', 'text/plain')
            return
        if text is None:
            resolved_master_url(state)
            text = _fetch(state.master_url, tag='handler:master', headers=state.headers)
        if not text:
            self._send(502, b'Bad gateway', 'text/plain')
            return

        # Base directory of the master URL (for resolving relative URIs).
        master_base = _base_url(text, state.master_url)

        out = []
        variant_urls = []
        audio_candidates = []
        lines = text.splitlines()
        logger.debug('%s: master playlist fetched, %s lines', channel_id, len(lines))
        i = 0
        while i < len(lines):
            line = lines[i]
            if line.startswith('#EXT-X-MEDIA:'):
                # Drop ALL EXT-X-MEDIA lines.  Audio renditions are merged
                # into the TS by the proxy; subtitle/CC renditions have
                # relative URIs that hlsdemux would resolve against the proxy
                # base URL (→ 404).  Collect audio rendition candidates for
                # merging - some channels carry more than one (e.g. a main
                # track plus hearing-impaired/descriptive audio), so the pick
                # is resolved below via select_primary_audio rather than just
                # keeping whichever line came last.
                if 'TYPE=AUDIO' in line:
                    cand = HLSPlaylist.audio_candidate(line, master_base)
                    if cand:
                        audio_candidates.append(cand)
                i += 1
                continue

            if line.startswith('#EXT-X-STREAM-INF:'):
                # Strip group references that no longer exist in the rewritten
                # master (AUDIO merged into TS, SUBTITLES/CLOSED-CAPTIONS dropped).
                clean = HLSPlaylist.remove_attr('AUDIO', line)
                clean = HLSPlaylist.remove_attr('SUBTITLES', clean)
                clean = HLSPlaylist.remove_attr('CLOSED-CAPTIONS', clean)
                out.append(clean)
                i += 1
                if i < len(lines):
                    real_url = urljoin(master_base, lines[i])
                    idx = len(variant_urls)
                    variant_urls.append(real_url)
                    proxy_url = (
                        f'http://{PROXY_HOST}:{PROXY_PORT}'
                        f'/pl/{channel_id}/{idx}.m3u8'
                    )
                    out.append(proxy_url)
                    i += 1
                continue

            out.append(line)
            i += 1

        audio_url = HLSPlaylist.select_primary_audio(audio_candidates)
        logger.debug('%s: audio renditions (uri, default, accessibility): %s', channel_id,
                     [(uri.split('?')[0].rsplit('/', 2)[-2], default, accessibility)
                      for uri, default, accessibility in audio_candidates])
        with state._lock:
            state.audio_url = audio_url
            state.variant_urls = variant_urls

        if audio_url:
            logger.debug('%s: audio rendition → %s', channel_id, audio_url)
            # Start the audio pre-fetch thread the first time audio_url is known.
            state.start_audio_prefetch_if_needed(channel_id)
        logger.debug('%s: %s variant(s) registered', channel_id, len(variant_urls))
        self._send(200, '\n'.join(out).encode(), 'application/x-mpegURL')

    # ── /pl/{channel_id}/{idx}.m3u8 ────────────────────────────────────────

    def _handle_playlist(self, channel_id: str, idx: int):
        state = _get_state(channel_id)
        if not state or idx >= len(state.variant_urls):
            self._send(404, b'Unknown variant', 'text/plain')
            return

        if not state.audio_url:
            # Not a demuxed channel – pass the real playlist through unchanged.
            text = _fetch(state.variant_urls[idx], tag='handler:playlist-passthrough', headers=state.headers)
            self._send(200, (text or '').encode(), 'application/x-mpegURL')
            return

        # Fetch the video variant playlist.  Audio segments are fetched
        # independently by the per-channel pre-fetch thread and stored in the
        # PTS ring; this handler only needs to know the video segment URLs.
        t_begin = time.monotonic()
        is_vod = channel_id.startswith('vod')
        audio_playlist = None
        if is_vod and state.audio_url and not state._audio_prepopulated:
            # A VOD playlist lists the whole asset and takes the CDN a while to produce: ask for the audio one
            # (needed below) while the video one is on its way.
            audio_playlist = _background(_fetch, state.audio_url, tag='handler:playlist-prepopulate',
                                         headers=state.headers)
        vtext = _fetch(state.variant_urls[idx], tag='handler:playlist', headers=state.headers)
        t_variant = time.monotonic()
        if not vtext:
            # A transient CDN blip on this fetch used to surface straight to
            # Enigma2 as a 502 on what looks like a routine manifest refresh
            # - suspected (see state._last_playlist_out's comment) of being
            # able to wedge GStreamer's hlsdemux outright rather than just
            # causing it to retry, unlike this same failure in the
            # background pre-fetch thread (which just waits for the next
            # poll with no consumer watching synchronously). Re-serving the
            # last successfully rewritten body for this variant - if we have
            # one - absorbs the blip invisibly instead of exposing it.
            cached = state._last_playlist_out.get(idx)
            if cached is not None:
                cached_out, cached_at = cached
                age = time.monotonic() - cached_at
                logger.debug('%s: Event: variant playlist fetch failed'
                             ' | Action: serving cached playlist (%.1fs stale) instead of 502',
                             channel_id, age)
                self._send(200, cached_out, 'application/x-mpegURL')
                return
            logger.debug('%s: Event: variant playlist fetch failed,'
                         ' no cached playlist available | Action: 502', channel_id)
            self._send(502, b'Video playlist unavailable', 'text/plain')
            return

        vbase = state.variant_urls[idx].split('?')[0].rsplit('/', 1)[0] + '/'
        vseq, vsegs = HLSPlaylist.segments(vtext, vbase)
        if vseq < 0:
            # CDN sent a bogus/negative #EXT-X-MEDIA-SEQUENCE (observed in
            # production). Serving it would bake negative segment numbers
            # into the playlist URLs handed to Enigma2, which then crash
            # AES128.iv()'s seq.to_bytes(16, 'big') on request. Treat it the
            # same as an unavailable playlist so the client retries shortly.
            logger.debug('%s: Event: negative #EXT-X-MEDIA-SEQUENCE (%s)'
                         ' from CDN playlist | Action: reject as unavailable, let client retry',
                         channel_id, vseq)
            self._send(502, b'Video playlist unavailable', 'text/plain')
            return
        # Update segment duration from the playlist EXTINF tags.  Live streams
        # use a constant duration; storing it per-channel means all time budgets
        # (poll intervals, fetch timeouts, audio wait) adapt automatically.
        _d = HLSPlaylist.extinf_duration(vtext)
        if _d is not None and _d != state.seg_duration:
            logger.debug('%s: seg_duration %.2fs -> %.2fs', channel_id, state.seg_duration, _d)
            state.seg_duration = _d

        # Pre-populate the audio ring synchronously on the FIRST playlist
        # request so that audio segments are available before the client
        # requests the first video segment.  On subsequent refreshes the
        # background pre-fetch thread keeps the ring topped up; we skip this
        # block to avoid redundant fetches.
        t_audio_playlist = t_audio = t_variant
        prepopulated = False
        if is_vod and not state.has_ready_segs():
            # The first video segments are not used before the audio is in, but nothing keeps them from being
            # downloaded meanwhile (the video loop takes them from here): that was a good part of the wait below.
            state._video_first_playlist = (state.variant_urls[idx], vtext)   # the video loop's first poll
            for url in vsegs[:_VOD_PRELOAD_SEGMENTS]:
                if url not in state._video_preload:
                    state._video_preload[url] = _background(_fetch, url, binary=True, timeout=state.seg_duration * 0.8,
                                                            tag='video-prefetch:segment', headers=state.headers)
        if state.audio_url and state.try_start_audio_prepopulate():
            prepopulated = True
            if audio_playlist is not None:
                atext_pre = audio_playlist.result()
            else:
                atext_pre = _fetch(state.audio_url, tag='handler:playlist-prepopulate', headers=state.headers)
            t_audio_playlist = time.monotonic()
            if atext_pre:
                abase_pre = state.audio_url.split('?')[0].rsplit('/', 1)[0] + '/'
                try:
                    aseq_pre, asegs_pre = HLSPlaylist.segments(atext_pre, abase_pre)
                    akey_pre, aiv_pre, adisc_pre = HLSPlaylist.segment_keys(atext_pre, abase_pre)
                except Exception:
                    asegs_pre = []
                    aseq_pre, akey_pre, aiv_pre, adisc_pre = 0, [], [], []

                # Cap how many segments this synchronous pass fetches. A live
                # playlist's sliding window is naturally small (a handful of
                # segments), but a VOD playlist lists the WHOLE asset up
                # front - fetching every one of those here would block this
                # HTTP response (hlsdemux waiting on the manifest) for as
                # long as the entire movie's audio track takes to download,
                # decrypt and PTS-scan, well past hlsdemux's own manifest
                # timeout (observed in production: "Invalid manifest" error
                # from hlsdemux ~15s in, while this loop was still working
                # through segment 100+ of a movie). Cap to a small head
                # start (_AUDIO_PREPOPULATE_CAP, well under _MAX_AUDIO_RING -
                # see its own comment for why) and let the background audio/
                # video pre-fetch threads (already started elsewhere, and
                # paced by _PREFETCH_LEAD_SEGMENTS) pick up everything beyond
                # this.
                prepopulate_cap = _VOD_AUDIO_PREPOPULATE_CAP if channel_id.startswith('vod') else _AUDIO_PREPOPULATE_CAP
                asegs_pre = asegs_pre[:prepopulate_cap]
                akey_pre = akey_pre[:prepopulate_cap]
                aiv_pre = aiv_pre[:prepopulate_cap]
                adisc_pre = adisc_pre[:prepopulate_cap]

                def _pre_fetch_one(seq, url, key_url, iv_explicit):
                    adata = _fetch(url, binary=True, timeout=state.seg_duration * 0.8,
                                   tag='handler:playlist-prepopulate-segment', headers=state.headers)
                    if not adata:
                        return
                    if key_url:
                        kb = AES128.fetch_key(key_url, _fetcher(state))
                        if kb:
                            adata = AES128.decrypt(adata, kb,
                                                   AES128.iv(iv_explicit, seq))
                    if not adata or adata[0] != 0x47:   # check after decryption
                        return
                    pre_results[seq] = adata

                pre_results: dict = {}
                workers = min(_AUDIO_FETCH_WORKERS, max(1, len(asegs_pre)))
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    for i, url in enumerate(asegs_pre):
                        k = akey_pre[i] if i < len(akey_pre) else None
                        iv = aiv_pre[i] if i < len(aiv_pre) else None
                        pool.submit(_pre_fetch_one, aseq_pre + i, url, k, iv)
                # Append in ascending seq order, only once the whole batch
                # has finished fetching (the `with` block above waits for
                # every submitted future) - see _append_audio's own
                # docstring for why order matters for a continuous buffer
                # in a way it never did for the old flat audio ring.
                for i in range(len(asegs_pre)):
                    seq = aseq_pre + i
                    adata = pre_results.get(seq)
                    if adata is None:
                        continue
                    disc = adisc_pre[i] if i < len(adisc_pre) else False
                    _append_audio(state, channel_id, seq, disc, adata)
                logger.debug('%s: pre-populated audio buffer with %s segment(s)',
                             channel_id, len(pre_results))
            t_audio = time.monotonic()

        # Start the video pre-fetch+mux thread on the first playlist request.
        # It polls the variant playlist independently, pre-muxes every new
        # segment (with up to 5 s wait for audio), and stores results in
        # state._ready_segs so _handle_segment can respond instantly.
        state.start_video_prefetch_if_needed(channel_id, idx)

        # Snapshot once per request, not per segment: whether this channel
        # has produced anything at all yet distinguishes "prefetch hasn't
        # caught up yet" (worth a short wait below) from a genuine cold
        # start, where nothing is ready yet and the existing _handle_segment
        # synchronous fallback is what's relied on to serve it - gating on
        # readiness here too would leave the very first playlist response
        # empty and stall live-start.
        if not state.has_ready_segs():
            # See _COLD_START_FIRST_SEGMENT_WAIT's own comment: wait here,
            # before replying at all, for the first output chunk to actually
            # be ready - rather than handing GStreamer a playlist it can act
            # on immediately but whose first segment isn't ready yet,
            # gambling on its patience for the *segment* fetch that follows.
            # A provisional (silence-muxed) chunk is still valid, parseable
            # TS bytes GStreamer can actually play - the late-audio upgrade
            # can still land afterward exactly as it would have anyway.
            # Best-effort: expiring just falls through with whatever's ready
            # (possibly nothing yet). For VOD, hold out for a real lookahead
            # window (_VOD_COLD_START_MIN_SEGMENTS), not just the first
            # segment - see that constant's own comment - using the longer
            # _VOD_COLD_START_WAIT budget to match.
            budget = _VOD_COLD_START_WAIT if is_vod else _COLD_START_FIRST_SEGMENT_WAIT
            # A diagnostic test (temporarily forcing this to 1) confirmed
            # the reported A/V offset persists even when this cold-start
            # window never advertises more than one segment - i.e. it's
            # independent of hlsdemux's skip-to-segment-1 behavior on a
            # multi-segment first reply, not caused by it. Safe to restore
            # the real fix for the separate cold-start stall this constant
            # exists for.
            min_segs = _VOD_COLD_START_MIN_SEGMENTS if is_vod else 1
            cold_deadline = time.monotonic() + budget
            t_wait = time.monotonic()
            while state.ready_seg_count() < min_segs and time.monotonic() < cold_deadline:
                time.sleep(0.05)
            if not is_vod and state.ready_seg_count() == 1 and 0 < state.first_ready_size() < _SLATE_WINDOW_BYTES:
                slate_deadline = time.monotonic() + _SLATE_WAIT
                while state.ready_seg_count() < 2 and time.monotonic() < slate_deadline:
                    time.sleep(0.05)
            logger.debug('%s: first playlist: variant playlist %.2fs, audio playlist %.2fs, audio segments %.2fs%s, '
                         'waited %.2fs for %s ready segment(s), %s ready',
                         channel_id, t_variant - t_begin, t_audio_playlist - t_variant, t_audio - t_audio_playlist,
                         '' if prepopulated else ' (not pre-populated)', time.monotonic() - t_wait, min_segs,
                         state.ready_seg_count())

        # Rewrite the video playlist: keep the CDN's own structural tags
        # (#EXTM3U, #EXT-X-VERSION, #EXT-X-TARGETDURATION, ...) but replace
        # #EXT-X-MEDIA-SEQUENCE and every segment/#EXTINF/#EXT-X-DISCONTINUITY
        # line with our own, built from state._ready_segs/_window_meta -
        # chunk_seq output windows no longer correspond 1:1 to CDN video
        # segments (see _cut_and_mux_next_window), so there's no longer a
        # per-CDN-segment line to rewrite in place; a chunk simply isn't
        # listed at all until it exists as a _ready_segs key, which replaces
        # the old explicit is_ready()/is_duplicate() withholding checks.
        # #EXT-X-KEY is dropped unconditionally - the proxy always decrypts
        # before serving, so hlsdemux must never attempt a second
        # decryption pass on the cleartext output.
        out = []
        for line in vtext.splitlines():
            stripped = line.strip()
            if (stripped.startswith('#EXT-X-KEY:')
                    or stripped == '#EXT-X-DISCONTINUITY'
                    or stripped.startswith('#EXT-X-MEDIA-SEQUENCE:')
                    or stripped.startswith('#EXTINF:')
                    # A VOD source's own manifest carries #EXT-X-PLAYLIST-TYPE:VOD
                    # and/or #EXT-X-ENDLIST - copying either straight through
                    # tells GStreamer's hlsdemux "this is the complete asset",
                    # taking whatever partial window this proxy has advertised
                    # so far (often just one chunk, this early in production) at
                    # face value as the *entire* movie. Confirmed in production:
                    # playback stops dead after exactly that one chunk, with no
                    # further playlist re-poll, because there was never a
                    # correctness reason for hlsdemux to ask for more. A live
                    # CDN playlist never carries either tag to begin with, so
                    # stripping them here is a no-op there.
                    or stripped.startswith('#EXT-X-PLAYLIST-TYPE:')
                    or stripped == '#EXT-X-ENDLIST'
                    or (stripped and not stripped.startswith('#'))):
                continue
            out.append(line)

        # Advertise roughly the same live-window size as the CDN's own
        # variant playlist (len(vsegs)) - Enigma2 expects a small sliding
        # window, not this proxy's full _MAX_SEG_CACHE-bounded history.
        window_size = max(1, len(vsegs))
        with state._lock:
            advertise_seqs = list(state._ready_segs.keys())[-window_size:]
            window_meta = dict(state._window_meta)

        media_seq = advertise_seqs[0] if advertise_seqs else state._next_chunk_seq
        insert_at = 1 if out and out[0].strip() == '#EXTM3U' else 0
        out.insert(insert_at, f'#EXT-X-MEDIA-SEQUENCE:{media_seq}')
        if channel_id.startswith('vod'):
            # Without #EXT-X-ENDLIST/#EXT-X-PLAYLIST-TYPE:VOD (deliberately
            # stripped above so hlsdemux keeps re-polling instead of treating
            # an early, partial window as the complete asset), this playlist
            # looks exactly like a live stream to hlsdemux - including on its
            # very first fetch, where a live-style client's own default is to
            # start playback a few segments back from the *end* of whatever
            # window it's just been handed (a live-safety margin), not at the
            # oldest/first entry. Confirmed in production: with 5 segments
            # advertised on the first reply (0-4, matching
            # _VOD_COLD_START_MIN_SEGMENTS - not a runaway production race),
            # Enigma2's first-ever request was for segment 1, silently
            # skipping segment 0. #EXT-X-START:TIME-OFFSET=0 is the standard
            # HLS tag for overriding exactly that default and telling the
            # client explicitly where to start; PRECISE=YES asks it to start
            # at that exact segment rather than rounding to a nearby one.
            # Live is unaffected (this only applies to a 'vod'-prefixed
            # channel_id) - a live zap starting near the live edge instead of
            # this proxy's own retained history is the correct behavior
            # there, not a bug to override.
            out.insert(insert_at + 1, '#EXT-X-START:TIME-OFFSET=0,PRECISE=YES')

        for cseq in advertise_seqs:
            meta = window_meta.get(cseq)
            if meta is None:
                continue
            duration = max(0.0, (meta.end_pts - meta.start_pts) / 90000.0)
            if meta.is_disc:
                out.append('#EXT-X-DISCONTINUITY')
            out.append(f'#EXTINF:{duration:.3f},')
            out.append(
                f'http://{PROXY_HOST}:{PROXY_PORT}'
                f'/seg/{channel_id}/{cseq}.ts'
            )

        if (channel_id.startswith('vod') and state._vod_complete and advertise_seqs
                and state.last_requested_seq() >= advertise_seqs[-1]):
            # ENDLIST was stripped above so hlsdemux wouldn't take an early,
            # partial window for the whole asset. Once every segment has been
            # produced AND Enigma2 has requested the very last one, tell it
            # where the stream really ends - without it, playback never
            # reaches EOS and the player hangs after the last segment instead
            # of closing. Not any earlier: the advertised playlist is only a
            # sliding window of the asset, and the moment hlsdemux sees
            # ENDLIST it treats that window as the complete asset, so sending
            # it while playback is still far from the end made playback stop
            # at the end of the window (confirmed: EOF at ~20:32 of a 22:20
            # stream, right after the full asset had been produced).
            out.append('#EXT-X-ENDLIST')

        out_bytes = '\n'.join(out).encode()
        state._last_playlist_out[idx] = (out_bytes, time.monotonic())
        self._send(200, out_bytes, 'application/x-mpegURL')

    # ── /seg/{channel_id}/{seq}.ts ─────────────────────────────────────────

    def _handle_segment(self, channel_id: str, seq: int):
        state = _get_state(channel_id)
        if not state:
            self._send(404, b'Unknown channel', 'text/plain')
            return

        state.record_segment_request(seq)
        if channel_id.startswith('vod'):
            state.evict_played(_VOD_KEEP_BEHIND_SEGMENTS)

        # Fast path: serve from the pre-muxed cache populated by the video
        # prefetch thread.  In steady state the chunk is already ready when
        # Enigma2 requests it (withheld from the served playlist until
        # produced - see _handle_playlist's readiness check; audio CDN lag
        # = 3.5-7 s typically, muxing completes well before the request). At
        # a content transition the matching audio may not have been
        # buffered yet (see _resolve_late_audio_window); the deadline below
        # gives that upgrade a real chance to land without holding this
        # response open so long that live delivery itself stalls - see
        # _HANDLE_SEGMENT_DEADLINE_FACTOR's comment for the incident that
        # capped it well under the full late-audio budget.
        # Read (not pop): a retried request for an already-served chunk
        # (e.g. Enigma2/gstreamer re-fetching after a playback stall) must
        # get the same correctly-muxed bytes back. The cache is already
        # capacity-bounded by _MAX_SEG_CACHE eviction in the prefetch loop.
        #
        # A provisional entry (silence-muxed, _resolve_late_audio_window
        # upgrade still in flight — see mark_provisional) is deliberately
        # NOT returned the instant it's seen: that raced the upgrade in
        # production (the placeholder gets served microseconds before the
        # real AV mux lands, and Enigma2 never re-requests a segment it's
        # already played). Keep polling until either the upgrade clears the
        # flag or this same deadline expires, then serve whatever's there.
        deadline = time.monotonic() + state.seg_duration * _HANDLE_SEGMENT_DEADLINE_FACTOR
        provisional_fallback = None
        while True:
            with state._lock:
                muxed = state._ready_segs.get(seq)
                provisional = seq in state._provisional_segs
            if muxed is not None and not provisional:
                logger.debug('%s/%s: cache hit %s bytes', channel_id, seq, len(muxed))
                out = state.timeline_adjusted(seq, muxed)
                state._last_segment_out = (seq, out)
                self._send(200, out, 'video/MP2T')
                return
            if muxed is not None:
                provisional_fallback = muxed
            if time.monotonic() >= deadline:
                break
            time.sleep(0.05)

        if provisional_fallback is not None:
            # Upgrade never landed within the deadline; serve the silence
            # version as a last resort rather than re-fetching/re-muxing on
            # demand below, which would just repeat the same full-budget
            # search _resolve_late_audio already ran and lost.
            logger.debug('%s/%s: cache hit %s bytes (provisional, upgrade never landed)',
                         channel_id, seq, len(provisional_fallback))
            out = state.timeline_adjusted(seq, provisional_fallback)
            state._last_segment_out = (seq, out)
            self._send(200, out, 'video/MP2T')
            return

        # Fallback: the prefetch loop never produced this chunk_seq at all
        # (rare: an initial zap-in burst race, or the prefetch threads
        # stalled). Unlike the old CDN-media-sequence keying, a chunk_seq
        # has no single CDN URL of its own to reconstruct from - it's cut
        # from continuous video/audio buffers that may have already moved
        # past whatever PTS range this chunk_seq would have covered, so
        # there's no way to deterministically regenerate it on demand the
        # way the old per-segment fallback could. Wait a little longer for
        # the prefetch loop to catch up (it may simply be a poll cycle
        # behind), then fall back to re-serving the last chunk actually
        # sent rather than attempting reconstruction - a brief stale-frame
        # repeat in this rare case is far better than a hard error that
        # risks wedging hlsdemux (see _serveLastSegment).
        logger.debug('%s/%s: Event: chunk never produced by prefetch loop'
                     ' | Action: wait a little longer, then re-serve last chunk', channel_id, seq)
        extra_deadline = time.monotonic() + state.seg_duration
        while time.monotonic() < extra_deadline:
            with state._lock:
                muxed = state._ready_segs.get(seq)
                provisional = seq in state._provisional_segs
            if muxed is not None and not provisional:
                logger.debug('%s/%s: cache hit %s bytes (after extended wait)',
                             channel_id, seq, len(muxed))
                out = state.timeline_adjusted(seq, muxed)
                state._last_segment_out = (seq, out)
                self._send(200, out, 'video/MP2T')
                return
            time.sleep(0.05)

        if self._serveLastSegment(state, channel_id, seq, 'chunk never produced'):
            return
        self._send(404, b'Segment not in cache', 'text/plain')

    def _serveLastSegment(self, state, channel_id, seq, reason):
        """Re-serve the last segment actually sent to Enigma2, in place of a
        hard 404/502, if we have one. See state._last_segment_out's comment -
        one repeated stale segment is a brief visible stutter, but far better
        than risking hlsdemux wedging on a hard error with no recourse but a
        manual re-zap. Returns True if it served something (caller should
        return without sending its own error response).

        Unlike the old per-CDN-segment design, there's no way to
        reconstruct a genuinely never-produced chunk_seq on demand here:
        chunk_seq is minted only at production time (see
        state._next_chunk_seq), so one that was never produced was never
        associated with any CDN URL to re-fetch from in the first place -
        not merely evicted from a cache, but never assigned a source at
        all. _handle_playlist only ever advertises chunk_seqs already
        present in state._ready_segs, so reaching this fallback with
        nothing to serve should be rare to the point of near-unreachability
        in practice; it exists purely as a last-resort backstop.
        """
        last = state._last_segment_out
        if last is None:
            # Nothing has ever actually been sent to Enigma2 yet (e.g. very
            # early in a session) - fall back to whatever's most recently
            # been *produced*, even if never yet requested, before giving
            # up to a hard 404 while genuinely available muxed data exists
            # right here in the cache.
            with state._lock:
                last = next(reversed(state._ready_segs.items()), None)
            if last is None:
                return False
            last = (last[0], state.timeline_adjusted(last[0], last[1]))  # raw from the cache, not yet shifted
        last_seq, last_bytes = last
        logger.debug('%s/%s: Event: %s | Action: re-serving last segment (%s) instead of erroring',
                     channel_id, seq, reason, last_seq)
        self._send(200, last_bytes, 'video/MP2T')
        return True

    # ── /rec/{channel_id}.ts ───────────────────────────────────────────────

    def _handle_recording(self, channel_id: str):
        # Deferred import: RecordingProxyUnmuxed imports from this module, so a
        # module-level import here would be circular.
        from . import RecordingProxyUnmuxed
        RecordingProxyUnmuxed.handle_recording(self, channel_id)

    # ── /mpl/{channel_id}.m3u8 and /mseg/{channel_id}/{seq}.ts ─────────────

    def _handle_muxed(self, channel_id: str, seq: 'int | None'):
        """Playlist (seq None) or segment of a muxed channel, see LiveProxyMuxed."""
        from . import LiveProxyMuxed  # deferred: LiveProxyMuxed imports from this module
        # keeps the idle reaper off a channel that is only played through LiveProxyMuxed
        _get_state(channel_id)
        if seq is None:
            LiveProxyMuxed.handle_playlist(self, channel_id)
        else:
            LiveProxyMuxed.handle_segment(self, channel_id, seq)

    # ── helpers ────────────────────────────────────────────────────────────

    def _send(self, code: int, body: bytes, content_type: str):
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


# ── Proxy server lifecycle ────────────────────────────────────────────────────

_server = None
_server_lock = threading.Lock()


def start():
    """Start the proxy HTTP server.  Safe to call multiple times (idempotent)."""
    global _server
    with _server_lock:
        if _server is not None:
            return
        try:
            _server = _ThreadingHTTPServer((PROXY_HOST, PROXY_PORT), HLSProxyHandler)
            t = threading.Thread(target=_server.serve_forever, daemon=True)
            t.name = 'StreamProxyHLS'
            t.start()
            threading.Thread(target=_reap_idle_channels, daemon=True,
                             name='LiveProxyReaper').start()
            logger.debug('HLS proxy started on %s:%s', PROXY_HOST, PROXY_PORT)
        except Exception as exc:
            logger.debug('HLS proxy start failed: %s', exc)
