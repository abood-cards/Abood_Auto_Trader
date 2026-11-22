import json
import unittest
from cloud_settings import defaults
from render_accounts import resolve_account, AccountConfigError
from chess_transport import ChessPocket


def frame(demo, uid=123):
    return '42'+json.dumps(['auth',{'isDemo':demo,'uid':uid,'session':'synthetic-test'}],indent=2)


class RenderTests(unittest.TestCase):
    def test_env_overrides_entire_saved_account(self):
        d=defaults(); d['demo_account']['ssid']=frame(1,999)
        env={'PO_SSID_DEMO':frame(1),'PO_UID_DEMO':'0'}
        c, source=resolve_account(d,'PRACTICE',env)
        self.assertEqual(source,'Render / ENV')
        self.assertEqual(json.loads(c['ssid'][2:])[1]['uid'],123)
        self.assertNotEqual(d['demo_account']['ssid'],c['ssid'])

    def test_independent_real_and_mongo_fallback(self):
        d=defaults(); d['demo_account']['ssid']=frame(1)
        env={'PO_SSID_REAL':frame(0),'PO_UID_REAL':'0'}
        self.assertEqual(resolve_account(d,'PRACTICE',env)[1],'MongoDB')
        self.assertEqual(resolve_account(d,'REAL',env)[1],'Render / ENV')

    def test_no_silent_fallback_or_secret_echo(self):
        d=defaults(); d['demo_account']['ssid']=frame(1)
        for env in ({'PO_SSID_DEMO':'private-invalid'}, {'PO_SSID_DEMO':frame(0)}, {'PO_WS_URL_DEMO':d['demo_account']['ws_url']}):
            with self.assertRaises(AccountConfigError) as ctx:
                resolve_account(d,'PRACTICE',env)
            self.assertNotIn('private-invalid',str(ctx.exception))
            self.assertNotIn('synthetic-test',str(ctx.exception))

    def test_raw_uid_requirement(self):
        d=defaults(); d['demo_account']['uid']=987
        with self.assertRaises(AccountConfigError):
            resolve_account(d,'PRACTICE',{'PO_SSID_DEMO':'synthetic-raw','PO_UID_DEMO':'0'})
        creds,_=resolve_account(d,'PRACTICE',{'PO_SSID_DEMO':'synthetic-raw','PO_UID_DEMO':'123'})
        self.assertEqual(json.loads(creds['ssid'][2:])[1]['uid'],123)

    def test_scope_balance_after_auth_and_ignore_unmarked_balance(self):
        p=ChessPocket(defaults()['demo_account'],'PRACTICE')
        p.process(['balance',{'demoBalance':55}]); self.assertFalse(p.account_verified)
        p.process(['successauth',{}]); p.process(['profile',{'user':{'balances':{'demo_balance':60,'real_balance':900}}}])
        self.assertEqual(p.verified_balance,60)
        p.process(['updateBalance',{'balance':999}]); self.assertEqual(p.verified_balance,60)
        p.process(['balance',{'balance':85,'isDemo':1}]); self.assertEqual(p.verified_balance,85)

    def test_unmarked_balance_and_catalog_not_authority(self):
        p=ChessPocket(defaults()['demo_account'],'PRACTICE')
        p.process(['successauth',{}]); p.process(['updateBalance',{'balance':999}])
        self.assertFalse(p.account_verified)
        self.assertIsNone(p.verified_balance)
