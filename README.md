# StreamProxyCockpit (SPC)
Local stream proxy for the Open-Enigma2 plugins that provide FAST-channel streams

## Features
- Muxes streams with a separate audio stream into single transport stream segments that the receiver plays
- Keeps ad breaks from disturbing playback and recordings
- Records a channel from one continuous stream, shared by all recordings and timeshifts of the channel
- Looks up the address of a channel when it is played, and renews it when it expires

## Used by
- PlutoTVCockpit
- RakutenTVCockpit
- SamsungTVCockpit

## Disclaimer
The project author is not responsible for how this software is used by others. It is not intended to be used for accessing or distributing copyrighted materials without authorization.
Users are solely responsible for determining the legality of their actions.

This repository has no control over the streams, links, or the legality of the content provided by the different hosts (including all mirror sites). It is the end user's responsibility to ensure the legal use of these streams, and we strongly recommend verifying that the content complies with all applicable laws, including copyright laws and regulations of your country's jurisdiction before use.

## Limitations
- Tested on OpenViX and OpenATV with DM900.

## Installation
The plugin will be installed as a dependency, no explicit installation required.

## Wiki
- https://github.com/OpenCockpit/StreamProxyCockpit/wiki
