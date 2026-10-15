# Copyright (C) 2026 by xcentaurix
# License: GNU General Public License v3.0
#
#   StreamProxy - the integration of the stream proxy (LiveProxyUnmuxed, RecordingProxyUnmuxed, ...) with Enigma2 for the
#   plugins that provide streams (any plugin that registers itself as a provider).
#
#   A provider plugin registers itself under a scheme:
#
#       StreamProxy.registerProvider("mytv", resolve)
#
#   Its bouquet entries then carry a lazy address "<scheme>%3a//<channel id>" as the stream of a 4097 service:
#
#       4097:0:1:<sid>:<tsid>:...:mytv%3a//<channel id>:<name>
#
#   so nothing has to be fetched while the bouquet is written, and the real address is looked up when the channel is
#   played or recorded. Enigma2 calls the service extensions below for every playService() and recordService();
#   they hand such a reference to the provider and replace it by the address of the proxy:
#
#       resolve(channel_id, sref) -> the address of the proxy for the channel, as returned by
#                                    LiveProxyUnmuxed.register_channel() (plus LiveProxyUnmuxed.header_suffix(headers) if the
#                                    stream needs request headers), or "" if the channel can't be resolved
#
#   Playback uses /auto/<channel id>.m3u8 of the proxy, a recording /rec/<channel id>.ts (RecordingProxyUnmuxed).
#   A channel is closed when the live slot moves on to another service, unless a recorder still uses it.

import re

from enigma import eServiceReference
from Navigation import Navigation

from .Debug import logger
from . import ConfigInit  # noqa: F401, pylint: disable=unused-import
from . import LiveProxyUnmuxed

_providers = {}   # scheme -> resolve(channel_id, sref)
_owners = {}      # channel id -> scheme of the provider that resolved it

# A channel id ends up in the path of an address of the proxy, so it must not need any quoting there.
_CHANNEL_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_PROXY_STREAM_RE = re.compile(r"/(?:auto|rec)/([A-Za-z0-9._-]+?)\.(?:m3u8|ts)$", re.IGNORECASE)

_live_channel_id = None  # channel id currently resolved for the live/foreground slot


def registerProvider(scheme, resolve):
    """Make the services "<scheme>%3a//<channel id>" playable and recordable through the proxy.

    Safe to call at import time of the provider plugin: the service extensions are installed on the class of
    Navigation, so they are in place before the session exists. A record timer that is resumed while Enigma2
    starts prepares its recording before any plugin's sessionstart runs, and would otherwise hand the recorder an
    address it can't play."""
    _providers[scheme.lower()] = resolve
    _applyRegistration()


def registerChannel(scheme, channel_id, master_url="", url_refresher=None, headers=None, refresh_on_failure=False,
                    session_param="sid"):
    """Start the proxy if it isn't running, register the channel with it and return the address Enigma2 plays it
    from (/auto/<id>.m3u8, plus the header suffix if the stream needs request headers).

    What a provider's resolve() returns. It makes no network call of its own: see LiveProxyUnmuxed.register_channel for
    resolving the stream address later (empty *master_url* and a *url_refresher*), and for *headers* and
    *refresh_on_failure* and *session_param*. *scheme* is the provider's: it lets the service extensions recognize
    the channel again when its service is a reference to the proxy's address (an instant recording, a player that
    plays the address itself)."""
    LiveProxyUnmuxed.start()
    _owners[channel_id] = scheme.lower()
    address = LiveProxyUnmuxed.register_channel(channel_id, master_url, url_refresher, headers, refresh_on_failure,
                                               session_param)
    if headers and not callable(headers):
        address += LiveProxyUnmuxed.header_suffix(headers)
    return address


def _applyRegistration():
    """Register the service extensions with Enigma2 (once there is a provider).

    playServiceExtension is what makes eServiceMP3 play the channels of the providers (through the proxy),
    recordServiceExtension what makes eServiceMP3Record record them. They are always installed: whether a provider's
    channels go through the proxy is decided by the provider itself (a setting of its own, or never optional), by what its bouquet holds - the lazy address "<scheme>%3a//<channel id>" or the address of the stream.
    A lazy address left over in a bouquet is resolved either way."""
    if not _providers:
        return
    for extensions, extension in (("playServiceExtensions", playServiceExtension),
                                  ("recordServiceExtensions", recordServiceExtension)):
        current = getattr(Navigation, extensions, None)
        if current is not None and extension not in current:
            current.append(extension)


def channelId(sref):
    """(scheme, channel id, lazy) of *sref*, or (None, None, False) if it isn't a service of a registered provider.
    *lazy* is True for the provider's own address, False for one that is already the address of the proxy.

    Recognizes the lazy address "<scheme>%3a//<id>" and a reference already rewritten to the proxy's /auto/<id>.m3u8
    or /rec/<id>.ts. The latter matters for an instant recording: unlike a timer it hands recordServiceExtension the
    currently playing service reference, which playServiceExtension has already rewritten to the local /auto/
    address, so the lazy address is gone. Only ids that a provider resolved before are accepted then.

    Pure parsing, no side effects: callers that only compare channel ids (see _isRecording) must not pay for a
    provider's lookup."""
    parts = sref.toString().split(":")
    if len(parts) <= 10:
        return None, None, False
    stream = parts[10]
    lowered = stream.lower()
    for scheme in _providers:
        prefix = scheme + "%3a//"
        if lowered.startswith(prefix):
            return scheme, stream[len(prefix):], True
    match = _PROXY_STREAM_RE.search(stream.partition("#")[0])  # without the header suffix of a provider
    if match and match.group(1) in _owners:
        return _owners[match.group(1)], match.group(1), False
    return None, None, False


def _resolve(sref):
    """Rewrite *sref* to the address of the proxy if it is a service of a provider.

    Returns (sref, channel_id_or_None) so callers can tell whether anything was resolved."""
    scheme, channel_id, lazy = channelId(sref)
    if channel_id is None:
        return sref, None
    if not lazy and LiveProxyUnmuxed.active_channel_url(channel_id):
        return sref, channel_id  # already the address of a running channel of the proxy
    if not _CHANNEL_ID_RE.match(channel_id):
        logger.error("channel id %r of %s can't be used in an address of the proxy", channel_id, scheme)
        return sref, None
    try:
        url = _providers[scheme](channel_id, sref)
    except Exception as exc:
        logger.error("%s can't resolve %s: %s", scheme, channel_id, exc)
        return sref, None
    if not url:
        return sref, None
    _owners[channel_id] = scheme
    parts = sref.toString().split(":")
    parts[10] = url.replace(":", "%3a")
    return eServiceReference(":".join(parts)), channel_id


def _isRecording(nav, channel_id):
    """True if a currently running, non-justplay record timer targets *channel_id*, or any recorder (e.g. a
    TimeshiftCockpit timeshift, which is not a timer) is connected to its /rec/ endpoint.

    Used to veto closing a channel that the live slot is leaving but that a background recording still depends on
    - see playServiceExtension."""
    if LiveProxyUnmuxed.is_recording_active(channel_id):
        return True
    for timer in nav.RecordTimer.timer_list:
        if timer.isRunning() and not timer.justplay:
            if channelId(timer.service_ref.ref)[1] == channel_id:
                return True
    return False


def playServiceExtension(_nav, sref, *_args, **_kwargs):
    """Called by Enigma2 for every playService() call, of any service type.

    Besides resolving the stream address, this is the only per-zap signal we get that the live slot has moved on:
    zapping to a service of no provider never reaches a provider at all, so without closing the previous channel
    here its pre-fetch threads would keep polling the CDN indefinitely. Skipped when a recorder still uses that
    channel (see _isRecording): closing it then would kill its pre-fetch threads mid-recording."""
    global _live_channel_id
    sref, channel_id = _resolve(sref)
    if (_live_channel_id and _live_channel_id != channel_id
            and not _isRecording(_nav, _live_channel_id)):
        LiveProxyUnmuxed.close_channel(_live_channel_id)
    _live_channel_id = channel_id
    return sref, False


def recordServiceExtension(_nav, sref, *_args, **_kwargs):
    """Rewrites a recording's sref to the RecordingProxyUnmuxed endpoint.

    _resolve() still runs first so the channel is registered and its pre-fetch threads are running exactly as for
    live playback. The result is then repointed from the segment-chunked HLS address (/auto/<id>.m3u8) live
    playback uses to RecordingProxyUnmuxed's continuous, container-harmonized re-mux endpoint (/rec/<id>.ts) - see
    RecordingProxyUnmuxed.py for why a recorded file needs different framing than a live HLS pipeline."""
    sref, channel_id = _resolve(sref)
    if channel_id is not None:
        rec_url = f"http://{LiveProxyUnmuxed.PROXY_HOST}:{LiveProxyUnmuxed.PROXY_PORT}/rec/{channel_id}.ts"
        parts = sref.toString().split(":")
        # a header suffix of the provider stays: Enigma2 sends those headers to the recorder's source as well
        suffix = parts[10].partition("#")[2]
        parts[10] = (rec_url + ("#" + suffix if suffix else "")).replace(":", "%3a")
        sref = eServiceReference(":".join(parts))
    return sref
