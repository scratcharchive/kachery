import os
import kachery as ka

def _add_download_args(parser):
    parser.add_argument('--load-from', '-l', help='Options: local, remote, remote_only', required=False, default='local')
    parser.add_argument('--preset', help='The name of the preset configuration', required=False, default=None)
    parser.add_argument('--url', help='The URL of the kachery database server to download from when loading from remote (or use KACHERY_URL environment variable)', required=False, default=None)
    parser.add_argument('--channel', '-c', help='The channel of the kachery database server to download from when loading from remote (or use KACHERY_CHANNEL environment variable)', required=False, default=None)
    parser.add_argument('--password', '-p', help='The password of the kachery database server to download from when loading from remote (or use KACHERY_PASSWORD environment variable)', required=False, default=None)

def _add_upload_args(parser):
    parser.add_argument('--store-to', '-s', help='Options: local, remote, remote_only', required=False, default='local')
    parser.add_argument('--preset', help='The name of the preset configuration', required=False, default=None)
    parser.add_argument('--url', help='The URL of the kachery database server to upload to when storing to remote (or use KACHERY_URL environment variable)', required=False, default=None)
    parser.add_argument('--channel', '-c', help='The channel of the kachery database server to upload to when storing to remote (or use KACHERY_CHANNEL environment variable)', required=False, default=None)
    parser.add_argument('--password', '-p', help='The password of the kachery database server to upload to when storing to remote (or use KACHERY_PASSWORD environment variable)', required=False, default=None)

def _set_download_config_from_parsed_args(args):
    url = args.url or os.getenv('KACHERY_URL', None)
    channel = args.channel or os.getenv('KACHERY_CHANNEL', None)
    password = args.password or os.getenv('KACHERY_PASSWORD', None)
    preset = args.preset or None

    if args.load_from == 'remote' or args.load_from == 'remote_only':
        if not preset:
            if not url:
                raise Exception('You must use the --preset or --url flag or set the KACHERY_URL environment variable')
            if not channel:
                raise Exception('You must use the --preset or --channel (-c) flag or set the KACHERY_CHANNEL environment variable')
            if not password:
                raise Exception('You must use the --preset or --password (-p) flag or set the KACHERY_PASSWORD environment variable')
    ka.set_config(
        preset=preset,
        url=url,
        channel=channel,
        password=password,
        load_from=args.load_from
    )

def _set_upload_config_from_parsed_args(args):
    url = args.url or os.getenv('KACHERY_URL', None)
    channel = args.channel or os.getenv('KACHERY_CHANNEL', None)
    password = args.password or os.getenv('KACHERY_PASSWORD', None)
    preset = args.preset or None

    if args.store_to == 'remote' or args.store_to == 'remote_only':
        if not preset:
            if not url:
                raise Exception('You must use the --preset or --url flag or set the KACHERY_URL environment variable')
            if not channel:
                raise Exception('You must use the --preset or --channel (-c) flag or set the KACHERY_CHANNEL environment variable')
            if not password:
                raise Exception('You must use the --preset or --password (-p) flag or set the KACHERY_PASSWORD environment variable')
    ka.set_config(
        preset=preset,
        url=url,
        channel=channel,
        password=password,
        store_to=args.store_to
    )