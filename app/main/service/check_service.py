import importlib
import tempfile

from azure.storage.fileshare import ShareServiceClient, ShareClient, ShareDirectoryClient, ShareFileClient
from azure.storage.fileshare._download import StorageStreamDownloader

conn_str = "BlobEndpoint=https://devdcmstorage.blob.core.windows.net/;QueueEndpoint=https://devdcmstorage.queue.core.windows.net/;FileEndpoint=https://devdcmstorage.file.core.windows.net/;TableEndpoint=https://devdcmstorage.table.core.windows.net/;SharedAccessSignature=sv=2019-10-10&ss=bfqt&srt=sco&sp=rwdlacupx&se=2022-07-16T07:57:54Z&st=2020-07-15T23:57:54Z&spr=https&sig=4cDoQPv%2Ba%2FQyBEFcr2pVojyMj4vgsm%2Fld6l9TPveQH0%3D"
service: ShareClient = ShareServiceClient.from_connection_string(conn_str).get_share_client('datachecks')


def get_domain_checks(domain_id):

    checks = get_check_from_shared() + get_check_from_shared(folder_name=domain_id)

    return checks


def get_check_from_shared(folder_name='default'):

    default_checks: ShareDirectoryClient = service.get_directory_client(folder_name)

    checks = []

    try:
        default_checks.create_directory()
    except Exception as e:
        pass

    for f in default_checks.list_directories_and_files():
        # DOWNLOAD FILE
        file: ShareFileClient = default_checks.get_file_client(f['name'])
        data: StorageStreamDownloader = file.download_file()

        # CREATE TEMP FILE
        file_stream = tempfile.NamedTemporaryFile(suffix='.py', delete=False)
        file_stream.write(data.readall())
        file_stream.flush()
        file_stream.close()

        # IMPORT MODULE
        spec = importlib.util.spec_from_file_location("datachecks", file_stream.name)
        datachecks_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(datachecks_module)
        datachecks_module.Check
        check = datachecks_module.Check

        # ADD CHECK
        checks.append(check)

    return checks
