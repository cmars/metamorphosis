from charmhelpers.core import hookenv
from charms.reactive import when, when_not, hook, set_state, remove_state
from charms.reactive.helpers import data_changed
from charms.layer.metamorphosis import Metamorphosis

from .autostart import autostart_service


@when('snap.installed.metamorphosis')
@when_not('kafka.ready')
def waiting_for_kafka():
    m = Metamorphosis()
    m.stop()
    hookenv.status_set('blocked', 'waiting for relation to kafka')


@when('kafka.joined')
@when_not('kafka.ready')
def wait_for_kafka(kafka):
    hookenv.status_set('waiting', 'waiting for Godot')


@when('snap.installed.metamorphosis')
@when_not('influxdb.connected')
def waiting_for_influxdb():
    m = Metamorphosis()
    m.stop()
    hookenv.status_set('blocked', 'waiting for relation to influxdb')


@when('kafka.ready', 'influxdb.available')
@when('snap.installed.metamorphosis')
def read(kafka, influxdb):
    set_state('metamorphosis.reconfigure')
    remove_state('metamorphosis.started')


@when('kafka.ready', 'influxdb.available')
@when('metamorphosis.reconfigure')
def configure(kafka, influxdb):
    hook_config = hookenv.config()
    topics_yaml = hook_config.get('topics_yaml', '')
    if not topics_yaml:
        hookenv.status_set('blocked', 'Please set the topics_yaml option')
        return

    config_changed = (data_changed('topics_yaml', topics_yaml) or
                      data_changed('kafka', kafka.kafkas()) or
                      data_changed('influx_user', influxdb.user()) or
                      data_changed('influx_password', influxdb.password()) or
                      data_changed('influx_host', influxdb.hostname()) or
                      data_changed('influx_port', influxdb.port()))
    if not config_changed:
        return

    hookenv.status_set('maintenance', 'updating configuration')

    m = Metamorphosis()
    m.stop()
    m.configure(kafka.kafkas(), influxdb, topics_yaml)
    # set app version string for juju status output
    snap_version = m.version() or 'unknown'
    hookenv.application_version_set(snap_version)
    set_state('metamorphosis.started')
    remove_state('metamorphosis.reconfigure')
    # update status from actual service-running state
    autostart_service()


@hook('config-changed')
def config_changed():
    hook_config = hookenv.config()
    if not data_changed('config', hook_config):
        return
    set_state('metamorphosis.reconfigure')
    remove_state('metamorphosis.started')
