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

## Installation
No installation required. Will be installed as dependency if required.

## Documentation
How it works, the settings, how a plugin registers its channels and troubleshooting are described in the [StreamProxyCockpit wiki](https://github.com/OpenCockpit/StreamProxyCockpit/wiki).

## Limitations
- Tested on OpenViX and OpenATV with DM900.
