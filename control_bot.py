"""Private owner-only Bot API controls; no MTProto or signal channel."""
import json
from contextlib import suppress
from aiogram import BaseMiddleware, Dispatcher
from aiogram.filters import CommandStart, Command
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from cloud_settings import auth_frame, valid_number, valid_url
from payout_guard import flag
import config
import scan_scope
from worker import account_key
from render_accounts import resolve_account, AccountConfigError


class PrivateOwner(BaseMiddleware):
    async def __call__(self, handler, event, data):
        msg = event.message if isinstance(event, CallbackQuery) else event
        user = getattr(event,'from_user',None)
        chat = getattr(msg,'chat',None)
        if user and chat and user.id == config.ADMIN_TELEGRAM_ID == chat.id and chat.type == 'private':
            return await handler(event,data)
        if isinstance(event,CallbackQuery):
            with suppress(Exception):
                await event.answer()


class Input(StatesGroup):
    value = State()


def buttons(*rows):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=t,callback_data=a) for t,a in row] for row in rows])


HOME = buttons((('▶️ بدء','start'),('⏹️ إيقاف','stop')),(('📡 الحالة','status'),('📊 الإحصائيات','stats')),(('⚙️ الإعدادات','settings'),))
BACK = (('🏠 الرئيسية','home'),)
SETTINGS = buttons((('👤 إدارة الحسابات','accounts'),('🎯 نطاق المسح','scan')),
                   (('💵 الأساس','edit:amount'),('🪜 مستويات التدبيل','edit:steps')),
                   (('🛟 تشغيل/إيقاف التعزيز','boost'),('➕ زيادة التعزيز','edit:increment')),
                   (('⚡ تدبيل استباقي (ديمو فقط)','early_double'),),
                   (('🛡 تشغيل/إيقاف الحدود','limits'),),
                   (('حد الربح','edit:profit'),('حد الخسارة','edit:loss')),BACK)
FIELDS = {'scan_symbols':'أرسل رموز الأصول كما تظهر في الحالة مفصولة بفاصلة أو مسافة، مثل EURUSD_otc، BTCUSD، #AAPL_otc (حتى 40 أصلًا).','ssid':'أرسل إطار auth للديمو، أو UID متبوعاً بمسافة ثم SSID الخام.', 'url':'أرسل رابط WebSocket للديمو.',
          'amount':'أرسل مبلغ الأساس بالدولار (0.01–100000).', 'steps':'أرسل عدد المستويات من 0 إلى 8. في الديمو السريع: 4 تعني أساس + 4 مضاعفات. في الوضع السابق: 4 تشمل صفقة الأساس.',
          'increment':'أرسل الزيادة فوق الأساس بالدولار.', 'profit':'أرسل حد الربح بالدولار.', 'loss':'أرسل حد الخسارة بالدولار.'}


def settings_text(d):
    t,r,k=d['trading'],d['recovery'],d['risk']
    try:
        _, source = resolve_account(d,d['account_mode'])
    except AccountConfigError as exc:
        source = str(exc)
    return (f"⚙️ M1 — {d['account_mode']}\nالأساس: ${t['trade_amount']:g} | المستويات: {t['martingale_steps']}\n"
            f"التعزيز: {'مفعّل' if r['mode']=='ENABLED' else 'معطّل'} | +${r['increment']:g}\n"
            f"نطاق المسح: {scan_scope.describe(d.get('scan', scan_scope.defaults()))} | عائد 90–92%\n"
            f"التدبيل الاستباقي للديمو: {'مفعّل' if d['chess']['early_double'] else 'معطّل'}\n"
            f"الحدود: {'مفعّلة' if k['mode']=='LIMITED' else 'معطّلة'} | +${k['profit_limit']:g} / −${k['loss_limit']:g}\n"
            f"مصدر الاتصال: {source}")


def scan_panel(d):
    scope=d.get('scan',scan_scope.defaults())
    groups=scope['groups'] if scope['mode']=='GROUPS' else []
    choices=[(('✅ ' if key in groups else '')+name,'scan_group:'+key) for key,name in scan_scope.LABELS.items()]
    rows=[tuple(choices[i:i+2]) for i in range(0,len(choices),2)]
    rows += [((('✅ ' if scope['mode']=='ALL' else '')+'جميع الأصول','scan_all'),
              (('✅ ' if scope['mode']=='CUSTOM' else '')+'قائمة مخصّصة','edit:scan_symbols')),
             (('⬅️ الإعدادات','settings'),)]
    text='🎯 نطاق المسح: '+scan_scope.describe(scope)+'\nاضغط على فئة لتفعيلها أو إلغائها. يمكنك الجمع بين أكثر من فئة.'
    if scope['mode']=='CUSTOM':
        text+='\n'+', '.join(scope['symbols'])
    return text,buttons(*rows)


def build_dispatcher(repo, worker, bot):
    dp = Dispatcher()
    dp.message.outer_middleware(PrivateOwner())
    dp.callback_query.outer_middleware(PrivateOwner())
    async def notice(text):
        await bot.send_message(config.ADMIN_TELEGRAM_ID,text)
    worker.notify = notice

    async def panel(cb,text,kb=HOME):
        from aiogram.exceptions import TelegramBadRequest
        try:
            await cb.message.edit_text(text,reply_markup=kb)
        except TelegramBadRequest as exc:
            if 'message is not modified' not in exc.message.lower():
                await cb.message.answer(text,reply_markup=kb)

    @dp.message(CommandStart())
    @dp.message(Command('cancel'))
    async def home(msg,state):
        await state.clear()
        await msg.answer('♟ عبود أوتو تريدر | Abood Auto Trader\nM1 · تجريبي / حقيقي',reply_markup=HOME)

    @dp.callback_query()
    async def callback(cb,state):
        action=cb.data or ''
        if action=='stop':
            worker.request_stop()
        await cb.answer()
        await state.clear()
        try:
            if action=='home':
                await panel(cb,'♟ عبود أوتو تريدر | Abood Auto Trader')
            elif action=='start':
                await panel(cb,await worker.start())
            elif action=='stop':
                await panel(cb,'مُنع فتح صفقات جديدة. تستمر مراقبة الصفقة المفتوحة حتى التسوية إن أمكن.')
            elif action=='stats':
                mode=repo.doc['account_mode']
                s=repo.doc['account_stats'][mode]
                await panel(cb,f"📊 {mode}\nالصفقات: {s['total_trades']} | ربح: {s['won_trades']} | خسارة: {s['lost_trades']} | تعادل: {s['draws']}\nسلاسل مستنفدة: {s['sequence_losses']}\nمبالغ الدخول: ${s['total_staked']:.2f} | العوائد: ${s['total_returned']:.2f}\nالصافي: ${s['net_pnl']:+.2f} | غير مؤكدة: {s['unconfirmed']}")
            elif action=='status':
                kb=buttons((('مراجعة الأرشفة','archive'),),BACK) if worker.pending else HOME
                await panel(cb,worker.status(),kb)
            elif action=='archive':
                await panel(cb,'أرشِف الأمر فقط بعد التحقق من أنه لم يعد مفتوحاً في سجل المنصة. سيُسجّل كغير مؤكد.',buttons((('تحققت — أرشف','archive:confirm'),),BACK))
            elif action=='archive:confirm':
                if worker.task and not worker.task.done():
                    await panel(cb,'انتظر انتهاء العامل أولاً.')
                else:
                    await repo.acknowledge_pending()
                    await panel(cb,'تمت الأرشفة. إذا بقي أمر آخر فراجعه من الحالة. لا يبدأ التداول تلقائياً.')
            elif action=='accounts':
                await panel(cb, f"الحساب المختار: {repo.doc['account_mode']}", buttons(
                    (('اختيار تجريبي','mode:PRACTICE'),('اختيار حقيقي','confirm_real')),
                    (('جلسة الديمو','edit:ssid:PRACTICE'),('رابط الديمو','edit:url:PRACTICE')),
                    (('جلسة الحقيقي','edit:ssid:REAL'),('رابط الحقيقي','edit:url:REAL')),
                    (('⬅️ الإعدادات','settings'),)))
            elif action=='confirm_real':
                await panel(cb,'اختيار الحساب الحقيقي يتيح استخدام أموال حقيقية بعد الضغط على بدء.',buttons((('تأكيد الحساب الحقيقي','mode:REAL'),),BACK))
            elif action in ('mode:PRACTICE','mode:REAL'):
                async with worker.lock:
                    if (worker.task and not worker.task.done()) or worker.pending:
                        raise ValueError
                    await repo.update({'account_mode':action.split(':')[1]})
                await panel(cb,settings_text(repo.doc),SETTINGS)
            elif action=='scan' or action=='scan_all' or action.startswith('scan_group:'):
                if action != 'scan':
                    async with worker.lock:
                        scope=repo.doc.get('scan',scan_scope.defaults())
                        if action=='scan_all':
                            value=scan_scope.defaults()
                        else:
                            key=action.split(':',1)[1]
                            if key not in scan_scope.LABELS:
                                raise ValueError
                            groups=set(scope['groups'] if scope['mode']=='GROUPS' else [])
                            groups.symmetric_difference_update({key})
                            if not groups:
                                await panel(cb,'لا يمكن إلغاء آخر فئة. اختر فئة أخرى أولاً أو اختر جميع الأصول.',scan_panel(repo.doc)[1])
                                return
                            value={'mode':'GROUPS','groups':[g for g in scan_scope.LABELS if g in groups],'symbols':[]}
                        await repo.update({'scan':value})
                await panel(cb,*scan_panel(repo.doc))
            elif action=='settings':
                await panel(cb,settings_text(repo.doc),SETTINGS)
            elif action=='early_double' and not repo.doc['chess']['early_double']:
                await panel(cb,'التدبيل الاستباقي تجريبي فقط: قد تفتح صفقتان وتتغير نتيجة الأولى قبل إغلاقها.', buttons((('تفعيل في الديمو','early_double:confirm'),),BACK))
            elif action in ('boost','limits','early_double','early_double:confirm'):
                d=repo.doc
                changes={'boost':{'recovery.mode':'TRADITIONAL' if d['recovery']['mode']=='ENABLED' else 'ENABLED'},
                         'early_double':{'chess.early_double':False},
                         'early_double:confirm':{'chess.early_double':True},
                         'limits':{'risk.mode':'TRADITIONAL' if d['risk']['mode']=='LIMITED' else 'LIMITED'}}
                await repo.update(changes[action])
                await panel(cb,settings_text(repo.doc),SETTINGS)
            elif action.startswith('edit:'):
                parts=action[5:].split(':')
                key=parts[0]
                mode=parts[1] if len(parts)==2 else 'PRACTICE'
                if key in FIELDS and mode in ('PRACTICE','REAL'):
                    await state.set_state(Input.value)
                    await state.update_data(key=key, mode=mode)
                    prompt = FIELDS[key].replace('للديمو', 'للحساب '+mode)
                    await panel(cb,prompt+'\n/cancel للإلغاء',buttons((('⬅️ الإعدادات','settings'),)))
        except ValueError:
            await panel(cb,'الإعداد غير صالح أو خارج النطاق.',SETTINGS)
        except Exception as exc:
            await panel(cb,'تعذّرت العملية: '+type(exc).__name__)

    @dp.message(Input.value)
    async def update(msg,state):
        data=await state.get_data()
        key=data.get('key')
        mode=data.get('mode','PRACTICE')
        raw=(msg.text or '').strip()
        try:
            if key in ('ssid','url'):
                import os
                suffix = 'REAL' if mode == 'REAL' else 'DEMO'
                if os.getenv(f'PO_SSID_{suffix}', '').strip():
                    await msg.answer('هذا الحساب مُدار عبر Render؛ عدّل متغيراته هناك ثم أعد النشر.')
                    await state.clear()
                    return
            if key=='ssid':
                if len(raw)>20000:
                    raise ValueError
                if raw.startswith('42['):
                    if flag(json.loads(raw[2:])[1].get('isDemo')) is not (mode=='PRACTICE'):
                        raise ValueError
                    frame=auth_frame(raw,mode)
                else:
                    uid, ssid = raw.split(maxsplit=1)
                    if not uid.isascii() or not uid.isdecimal() or ssid.startswith('42'):
                        raise ValueError
                    frame=auth_frame(ssid,mode,int(uid))
                async with worker.lock:
                    if (worker.task and not worker.task.done()) or worker.pending:
                        raise ValueError
                    await repo.update({account_key(mode)+'.ssid':frame})
            elif key=='url':
                async with worker.lock:
                    if (worker.task and not worker.task.done()) or worker.pending:
                        raise ValueError
                    await repo.update({account_key(mode)+'.ws_url':valid_url(raw)})
            elif key=='scan_symbols':
                async with worker.lock:
                    await repo.update({'scan':{'mode':'CUSTOM','symbols':scan_scope.parse_symbols(raw),'groups':[]}})
            elif key=='steps':
                await repo.update({'trading.martingale_steps':int(raw)})
            elif key in ('amount','increment','profit','loss'):
                fields={'amount':'trading.trade_amount','increment':'recovery.increment','profit':'risk.profit_limit','loss':'risk.loss_limit'}
                await repo.update({fields[key]:valid_number(raw,.01,100000 if key in ('amount','increment') else 1000000)})
            else:
                raise ValueError
        except (ValueError,TypeError,IndexError,KeyError,AttributeError):
            await msg.answer('لم يُحفظ الإعداد. تحقّق من القيمة؛ تعديل الاتصال يتطلب إيقاف العامل وعدم وجود أمر معلّق.')
            return
        except Exception as exc:
            await msg.answer('تعذّر الحفظ: '+type(exc).__name__)
            return
        finally:
            if key=='ssid':
                with suppress(Exception):
                    await msg.delete()
        await state.clear()
        if key=='scan_symbols':
            text,kb=scan_panel(repo.doc)
            await msg.answer(text,reply_markup=kb)
        else:
            await msg.answer(settings_text(repo.doc),reply_markup=SETTINGS)
    return dp
