# Copyright (C) 2018-2026 by xcentaurix
# License: GNU General Public License v3.0


from Components.config import config, ConfigYesNo
from .Debug import logger  # noqa: F401, pylint: disable=unused-import  # creates config.plugins.streamproxycockpit


# Whether the plugin registers its service extensions for playback (eServiceMP3) and recording (eServiceMP3Record).
# Switched off, the services of the provider plugins are no longer resolved to the stream proxy for that purpose.
config.plugins.streamproxycockpit.playback = ConfigYesNo(default=True)
config.plugins.streamproxycockpit.recording = ConfigYesNo(default=True)
