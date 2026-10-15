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

## Limitations
- Tested on OpenViX and OpenATV with DM900.

## Installation
The plugin will be installed as a dependency, no explicit installation required.

## Wiki
- https://github.com/OpenCockpit/StreamProxyCockpit/wiki
