# Copyright (C) 2018-2026 by xcentaurix
# License: GNU General Public License v3.0


from Components.config import config
from Screens.Setup import Setup
from . import _
from . import ConfigInit  # noqa: F401, pylint: disable=unused-import
from .Debug import log_levels, setLogLevel
from .Version import PLUGIN


class SetupScreen(Setup):

    def __init__(self, session):
        Setup.__init__(self, session, setup="streamproxycockpit", plugin="SystemPlugins/StreamProxyCockpit", PluginLanguageDomain=PLUGIN)
        self.setTitle(PLUGIN + " - " + _("Setup"))

    def keySave(self):
        # the registration follows the settings as they change (see StreamProxy), only the log level needs applying
        setLogLevel(log_levels[config.plugins.streamproxycockpit.debug_log_level.value])
        Setup.keySave(self)
