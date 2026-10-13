# Copyright (C) 2018-2026 by xcentaurix
# License: GNU General Public License v3.0


from Plugins.Plugin import PluginDescriptor
from . import _
from .Debug import logger
from .SetupScreen import SetupScreen
from .Version import PLUGIN, VERSION


def openSettings(session, **__kwargs):
    session.open(SetupScreen)


def autoStart(reason, **__kwargs):
    if reason == 0:  # startup
        logger.info("+++ Version: %s starts...", VERSION)
    elif reason == 1:  # shutdown
        logger.info("--- shutdown")


def Plugins(**__kwargs):
    return [
        PluginDescriptor(
            where=PluginDescriptor.WHERE_AUTOSTART,
            fnc=autoStart,
            needsRestart=True
        ),
        PluginDescriptor(
            name=PLUGIN + " - " + _("Setup"),
            description=_("Settings of the stream proxy"),
            icon="plugin.png",
            where=PluginDescriptor.WHERE_PLUGINMENU,
            fnc=openSettings
        ),
    ]
