from charmhelpers.core import hookenv

from charms.reactive import hook

from charms.layer import snap


@hook('stop')
def uninstall():
    try:
        snap.remove('kpi-exporter')
    except Exception as e:
        # log errors but do not fail stop hook
        hookenv.log('failed to remove snap: {}'.format(e), hookenv.ERROR)
