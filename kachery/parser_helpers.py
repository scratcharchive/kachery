import os
import kachery as ka

def _add_download_args(parser):
    parser.add_argument('--fr', '-f', help='Where to load from', required=False, default='')
    parser.add_argument('--remote-only', action='store_true', help='Whether to only load from remote (good for debugging)')
    parser.add_argument('--url', help='The URL of the kachery database server to download from when loading from remote (or use KACHERY_URL environment variable)', required=False, default=None)
    parser.add_argument('--channel', '-c', help='The channel of the kachery database server to download from when loading from remote (or use KACHERY_CHANNEL environment variable)', required=False, default=None)
    parser.add_argument('--password', '-p', help='The password of the kachery database server to download from when loading from remote (or use KACHERY_PASSWORD environment variable)', required=False, default=None)

def _add_upload_args(parser):
    parser.add_argument('--to', '-t', help='Where to store to', required=False, default='')
    parser.add_argument('--remote-only', action='store_true', help='Whether to only store to remote (good for saving disk space)')
    parser.add_argument('--url', help='The URL of the kachery database server to upload to when storing to remote (or use KACHERY_URL environment variable)', required=False, default=None)
    parser.add_argument('--channel', '-c', help='The channel of the kachery database server to upload to when storing to remote (or use KACHERY_CHANNEL environment variable)', required=False, default=None)
    parser.add_argument('--password', '-p', help='The password of the kachery database server to upload to when storing to remote (or use KACHERY_PASSWORD environment variable)', required=False, default=None)

def _set_download_config_from_parsed_args(args):
    fr = args.fr or None
    url = args.url or None
    channel = args.channel or None
    password = args.password or None
    remote_only = args.remote_only

    if fr is not None:
        if url is not None or channel is not None or password is not None:
            raise Exception('Cannot use --url or --channel or --password together with --fr')
        ka.set_config(
            fr=fr,
            from_remote_only = remote_only
        )
    else:
        ka.set_config(
            fr=dict(
                url=url,
                channel=channel,
                password=password
            ),
            from_remote_only = remote_only
        )

def _set_upload_config_from_parsed_args(args):
    to = args.to or None
    url = args.url or None
    channel = args.channel or None
    password = args.password or None
    remote_only = args.remote_only

    if to is not None:
        if url is not None or channel is not None or password is not None:
            raise Exception('Cannot use --url or --channel or --password together with --to')
        ka.set_config(
            to=to,
            to_remote_only = remote_only
        )
    else:
        ka.set_config(
            to=dict(
                url=url,
                channel=channel,
                password=password
            ),
            to_remote_only = remote_only
        )