"""Resolve broker connection settings without writing deployment secrets to MongoDB."""
import json
import os
import config
from payout_guard import flag
from urllib.parse import urlparse
from cloud_settings import auth_frame, valid_url


def key_for(mode):
    return 'real_account' if mode == 'REAL' else 'demo_account'


class AccountConfigError(ValueError):
    pass


def resolve_account(doc, mode, env=None):
    env = os.environ if env is None else env
    suffix = 'REAL' if mode == 'REAL' else 'DEMO'
    names = [f'PO_{part}_{suffix}' for part in ('SSID','WS_URL','UID')]
    values = [str(env.get(k,'')).strip() for k in names]
    stored = doc[key_for(mode)]
    ssid, url, uid = values
    source = 'Render / ENV' if ssid else 'MongoDB'
    if ssid:
        url = url or (config.REAL_URL if mode == 'REAL' else config.DEMO_URL)
    else:
        if url or uid not in ('','0'):
            raise AccountConfigError(f'أضف {names[0]} مع إعدادات Render لهذا الحساب.')
        ssid, url, uid = stored.get('ssid',''), stored.get('ws_url',''), stored.get('uid',0)
    try:
        url = valid_url(url)
        host = urlparse(url).hostname or ''
        if mode == 'REAL' and host.startswith('demo-'):
            raise ValueError
    except ValueError:
        raise AccountConfigError(f'راجع {names[1]} للحساب المختار.') from None
    try:
        # A complete frame supplies its own UID. Do not borrow a UID from an old
        # MongoDB credential when a new raw SSID has been supplied in ENV.
        ssid = ssid.strip().strip("'\"")
        if ssid.startswith('42'):
            payload = json.loads(ssid[2:])[1]
            if flag(payload.get('isDemo')) is not (mode == 'PRACTICE'):
                raise AccountConfigError(f'نوع إطار {names[0]} لا يطابق الحساب المختار.')
            uid = 0
        else:
            uid = int(uid or 0)
        frame = auth_frame(ssid,mode,uid)
    except AccountConfigError:
        raise
    except (ValueError, TypeError, OverflowError, AttributeError, IndexError, KeyError):
        raise AccountConfigError(f'راجع {names[0]}؛ يلزم إطار auth كامل، أو SSID خام مع {names[2]} الفعلي.') from None
    return {'ssid':frame,'ws_url':url,'uid':uid}, source
