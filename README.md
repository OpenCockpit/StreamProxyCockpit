# StreamProxyCockpit (SPC)
Local stream proxy for the Open-Enigma2 plugins that provide FAST-channel streams

## Features
- HLS proxy on `127.0.0.1:7654` that turns demuxed streams (video and audio as separate streams) into single muxed TS segments Enigma2 can play, and re-muxes already muxed streams into container-continuous segments
- Recording endpoint that writes one continuous MPEG-TS stream per recording, shared by several recordings and timeshifts of the same channel
- Per channel request headers, and a stream address that is resolved when the channel is first used
- Service extensions for Enigma2 that resolve the channels of the provider plugins when they are played or recorded

## Used by
- PlutoTVCockpit
- RakutenTVCockpit
- SamsungTVCockpit

## For plugin authors
A provider registers itself under a scheme when its plugin is loaded:

```python
from Plugins.SystemPlugins.StreamProxyCockpit import StreamProxy

def resolveStream(channel_id, sref):
    # runs on the main thread while Enigma2 zaps: do not wait for the network here
    return StreamProxy.registerChannel("mytv", channel_id, url_refresher=lookupStreamUrl, refresh_on_failure=True)

StreamProxy.registerProvider("mytv", resolveStream)
```

Its bouquet entries use `mytv%3a//<channel id>` as the stream of a 4097 service. When such a service is played or recorded, Enigma2 calls `resolveStream(channel_id, sref)`, which returns the address of the proxy for the channel. `StreamProxy.registerChannel()` starts the proxy, registers the channel and returns that address.

The service extensions are always installed once a provider has registered. Whether the channels of a provider go through the proxy is the provider's decision, and it is made when it writes its bouquet: the lazy address for a provider that needs the proxy or has it switched on (an own setting "Stream proxy" in PlutoTVCockpit, RakutenTVCockpit and SamsungTVCockpit), otherwise the address of the stream itself, the way it was before the stream proxy existed. A lazy address that is still in a bouquet is resolved either way.

| Argument | Meaning |
|---|---|
| `scheme` | Scheme of the provider |
| `channel_id` | Id of the channel; letters, digits and `._-` only, it is part of an address of the proxy |
| `master_url` | Master playlist of the stream; empty (default) if it has to be looked up first |
| `url_refresher` | `callable() -> master url`; called on a worker thread, so it may block. It resolves an empty master URL and renews an address that no longer works |
| `headers` | Request headers (dict) the proxy sends for this channel, or a callable returning them (runs on a worker thread) |
| `refresh_on_failure` | Call `url_refresher` whenever the master playlist can't be fetched (for addresses that expire) |

If the stream needs request headers for Enigma2 itself (a stream the proxy hands over unchanged), append `LiveProxyUnmuxed.header_suffix(headers)` to the returned address; `registerChannel()` does that for a plain dict.

Playback then uses `/auto/<channel id>.m3u8` of the proxy, a recording `/rec/<channel id>.ts`. The channel of the live slot is closed when Enigma2 zaps to another service, unless a recorder still uses it. The service extensions are installed while the provider plugin is loaded, so a record timer that is resumed while Enigma2 starts finds them in place.

## Settings
The setup screen is in the plugin menu (**StreamProxyCockpit - Setup**):

| Setting | Meaning |
|---|---|
| **Log level** | Debug log level of the proxy (expert setting) |

Whether a TV plugin uses the proxy is set in that plugin: **Stream proxy** in the setup of PlutoTVCockpit, RakutenTVCockpit and SamsungTVCockpit (on by default). The bouquet of each setting is kept (`<bouquet>.tv.proxy`, `<bouquet>.tv.noproxy`), `<bouquet>.tv` is always the one of the current setting: changing it copies the stored bouquet over `<bouquet>.tv`, a bouquet that is not stored yet or is older than the update interval (5 hours) is generated.

## Installation
The package `enigma2-plugin-systemplugins-streamproxycockpit` is installed together with the plugins that need it. Restart Enigma2 afterwards.

## Debugging
The proxy writes its debug messages with the prefix `SPC:`. Set the log level in the settings of the plugin that uses it, or `config.plugins.streamproxycockpit.debug_log_level`. `AnalyzeProxyLog.py` summarises such a log by hand:

```
python3 AnalyzeProxyLog.py /path/to/Enigma2_debug_<date>.log
```

## Disclaimer
The project author is not responsible for how this software is used by others. It is not intended to be used for accessing or distributing copyrighted materials without authorization.
Users are solely responsible for determining the legality of their actions.

This repository has no control over the streams, links, or the legality of the content provided by the different hosts (including all mirror sites). It is the end user's responsibility to ensure the legal use of these streams, and we strongly recommend verifying that the content complies with all applicable laws, including copyright laws and regulations of your country's jurisdiction before use.

## Limitations
- Tested on OpenViX and OpenATV with DM900.

## Links
- Installation: https://OpenCockpit.github.io/StreamProxyCockpit
