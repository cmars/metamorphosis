from charmhelpers.core import hookenv

from charms.reactive import when

from charms.layer.metamorphosis import Metamorphosis


@when('metamorphosis.started')
def autostart_service():
    '''
    Attempt to restart the service if it is not running.
    '''
    m = Metamorphosis()

    if m.is_running():
        hookenv.status_set('active', 'ready')
        return
    elif m.is_autostart_disabled():
        hookenv.status_set('blocked',
                           'metamorphosis not running; autostart disabled')
        return

    for i in range(3):
        hookenv.status_set(
            'maintenance',
            'attempting to restart metamorphosis, '
            'attempt: {}'.format(i+1)
        )
        m.restart()
        if m.is_running():
            hookenv.status_set('active', 'ready')
            return

    hookenv.status_set('blocked',
                       'failed to start metamorphosis; check syslog')
