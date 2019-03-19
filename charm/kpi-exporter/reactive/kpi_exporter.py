import glob
import os

from subprocess import check_call

from charmhelpers.core import hookenv
from charms.reactive import when, when_not, hook, set_state, remove_state
from charms.reactive.helpers import data_changed
from charmhelpers.core.hookenv import config
from charms.layer.kpiexporter import KPI_EXPORTER_SNAP, KPIExporter


@when_not('kpi-exporter.available')
def install():
    install_snap()


@hook('upgrade-charm')
def upgrade():
    upgrade_snap()


@hook('stop')
def uninstall():
    check_call(['snap', 'remove', "kpi-exporter"])


def install_snap():
    # Need to install the core snap explicit. If not, there's
    # no slots for removable-media on a bionic install.
    # Not sure if that's a snapd bug or intended behavior.
    check_call(['snap', 'install', 'core'])

    cfg = config()
    # KSQL-Server's snap presedence is:
    # 1. Included in snap
    # 2. Included with resource of charm of 'ksql-server'
    # 3. Snap store with release channel specified in config
    snap_file = get_snap_file_from_charm() or hookenv.resource_get('kpi-exporter')
    if snap_file:
        check_call(['snap', 'install', '--dangerous', snap_file])
    if not snap_file:
        check_call(
            [
                'snap',
                'install',
                '--{}'.format(cfg['release-channel']),
                KPI_EXPORTER_SNAP
            ]
        )

    set_state('kpi-exporter.available')


def upgrade_snap():
    cfg = config()
    check_call(
        [
            'snap',
            'refresh',
            '--{}'.format(cfg['release-channel']),
            KPI_EXPORTER_SNAP
        ]
    )
    set_state('ksql.available')


@when('kpi-exporter.available')
@when_not('kafka.joined')
def waiting_for_kafka():
    k = KPIExporter()
    k.stop()
    hookenv.status_set('blocked', 'waiting for relation to kafka')


@when('kafka.joined')
@when_not('kafka.ready')
def wait_for_kafka(kafka):
    hookenv.status_set('waiting', 'waiting for Kafka to become ready')


@when('kpi-exporter.available')
@when_not('influxdb.connected')
def waiting_for_influxdb():
    k = KPIExporter()
    k.stop()
    hookenv.status_set('blocked', 'waiting for relation to influxdb')


@when('kafka.ready', 'influxdb.available')
@when_not('kpi-exporter.ready')
def configure(kafka, influxdb):
    config = hookenv.config()
    topics_yaml = config.get('topics_yaml', '')
    if not topics_yaml:
        hookenv.status_set('blocked', 'Please set the topics_yaml option')
        return

    kpi = KPIExporter()
    kpi.configure(kafka.kafkas(), influxdb, topics_yaml)
    set_state('kpi-exporter.ready')
    hookenv.status_set('active', 'ready')
    # set app version string for juju status output
    kpi_version = kpi.version() or 'unknown'
    hookenv.application_version_set(kpi_version)


@hook('config-changed')
def config_changed():
    config = hookenv.config()
    if not data_changed('config', config):
        return
    kpi = KPIExporter()
    kpi.stop()
    remove_state('kpi-exporter.ready')


def get_snap_file_from_charm():
    snap_files = sorted(glob.glob(os.path.join(
        hookenv.charm_dir(), "{}*.snap".format(KPI_EXPORTER_SNAP))))[::-1]
    if not snap_files:
        return None
    return snap_files[0]
