import os
import shutil

from charmhelpers.core import hookenv

from charms.reactive import (when, when_file_changed, remove_state,
                             when_not, set_state)

from charms.layer import tls_client
from charms.layer.metamorphosis import (METAMORPHOSIS_CA_CERT,
                                        METAMORPHOSIS_CERT,
                                        METAMORPHOSIS_KEY)


@when('certificates.available')
def send_data():
    assertDirExists(METAMORPHOSIS_CERT)
    # Request a server cert with this information.
    tls_client.request_client_cert(
        hookenv.service_name(),
        crt_path=METAMORPHOSIS_CERT,
        key_path=METAMORPHOSIS_KEY)


@when_file_changed(
    METAMORPHOSIS_CA_CERT, METAMORPHOSIS_CERT, METAMORPHOSIS_KEY)
def restart_when_cert_key_changed():
    remove_state('metamorphosis.started')
    set_state('metamorphosis.reconfigure')


@when('tls_client.ca_installed')
@when_not('metamorphosis.ca.keystore.saved')
def import_ca_crt_to_keystore():
    ca_path = '/usr/local/share/ca-certificates/{}.crt'.format(
        hookenv.service_name()
    )

    if os.path.isfile(ca_path):
        shutil.copyfile(ca_path, METAMORPHOSIS_CA_CERT)
        remove_state('tls_client.ca_installed')
        set_state('metamorphosis.ca.keystore.saved')


def assertDirExists(path):
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path), exist_ok=True)
