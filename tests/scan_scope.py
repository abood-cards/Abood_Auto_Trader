"""Persisted market selection and deterministic circular traversal."""
import re
from payout_guard import symbol

LABELS = {'forex':'💱 العملات', 'crypto':'🪙 الكريبتو', 'stocks':'📈 الأسهم',
          'indices':'📊 المؤشرات', 'commodities':'🛢 السلع', 'other':'🗂 أخرى'}
FX = frozenset('USD EUR GBP JPY AUD CAD CHF NZD CNY CNH AED BHD KES NZD OMR QAR NGN SAR JOD NOK SEK DKK SGD INR IDR BRL MXN PLN HUF CZK HKD ZAR TRY UAH CLP COP ARS PKR PHP TWD THB VND RUB EGP KRW MYR ILS BDT KWD'.split())
CRYPTO = frozenset('BTC ETH SOL XRP DOGE ADA BNB AVAX TON TRX BCH LTC DOT LINK SHIB PEPE SUI XLM XMR ETC AAVE ONDO UNI NEAR USDT USDC MATIC ATOM DASH ICP FIL BOME WIF'.split())


def defaults():
    return {'mode':'ALL','groups':[],'symbols':[]}


def validate(scope):
    if not isinstance(scope,dict) or scope.get('mode') not in ('ALL','GROUPS','CUSTOM'):
        raise ValueError('invalid_scan_mode')
    for key,limit in (('groups',len(LABELS)),('symbols',40)):
        values=scope.get(key)
        if not isinstance(values,list) or len(values)>limit or any(not isinstance(v,str) for v in values) or len(values)!=len(set(values)):
            raise ValueError('invalid_scan_selection')
        if not all(v in LABELS if key=='groups' else symbol(v)==v for v in values):
            raise ValueError('invalid_scan_value')
    if (scope['mode']=='GROUPS' and not scope['groups']) or (scope['mode']=='CUSTOM' and not scope['symbols']):
        raise ValueError('empty_scan_scope')


def category(asset, hint=None):
    if isinstance(hint,str):
        tag=re.sub(r'[^a-z]','',hint.lower())
        groups={'forex':('forex','currency','currencies','fx'), 'crypto':('crypto','cryptocurrency','cryptocurrencies','cryptos'),
                'stocks':('stock','stocks','equity','equities','share','shares'), 'indices':('indices','index','indexes'),
                'commodities':('commodities','commodity','metals','energies')}
        for group,tags in groups.items():
            if tag in tags:
                return group
    base=re.sub(r'_?OTC$','',asset.upper())
    compact=re.sub(r'[/_-]','',base)
    if len(compact)==6 and compact[:3] in FX and compact[3:] in FX:
        return 'forex'
    if compact in CRYPTO or any(compact.startswith(c) and compact[len(c):] in (FX | CRYPTO) for c in CRYPTO):
        return 'crypto'
    if compact.startswith(('XAU','XAG','XPT','XPD')) or compact in {'UKBRENT','USCRUDE','BRENT','WTI','NATGAS','GOLD','SILVER','PALLADIUM','PLATINUM','COPPER'}:
        return 'commodities'
    if compact in {'US100','US30','US500','US2000','SP500','NASDAQ','NDX','DJI','DAX','GER40','DE40','FRA40','FR40','FTSE100','UK100','JPN225','JP225','AUS200','AU200','HKG33','HK33','EU50','EUSTX50'}:
        return 'indices'
    if base.startswith('#') and re.fullmatch(r'#[A-Z][A-Z0-9.]{0,12}',base):
        return 'stocks'
    return 'other'


def allowed(scope, asset, groups):
    return (scope['mode']=='ALL' or
            (scope['mode']=='CUSTOM' and asset.casefold() in {a.casefold() for a in scope['symbols']}) or
            (scope['mode']=='GROUPS' and groups.get(asset,category(asset)) in scope['groups']))


def circular(assets, after):
    names=sorted(set(assets))
    if after is None:
        return names
    return [a for a in names if a>after]+[a for a in names if a<=after]


def describe(scope):
    if scope['mode']=='GROUPS':
        return '، '.join(LABELS[g] for g in scope['groups'])
    if scope['mode']=='CUSTOM':
        return f"قائمة مخصّصة ({len(scope['symbols'])})"
    return 'جميع الأصول'


def parse_symbols(text):
    symbols={}
    for token in re.split(r'[,،;\s]+',text.strip()):
        if not token:
            continue
        if symbol(token)!=token:
            raise ValueError('invalid_scan_symbol')
        symbols.setdefault(token.casefold(),token)
    values=list(symbols.values())
    validate({'mode':'CUSTOM','groups':[],'symbols':values})
    return values
