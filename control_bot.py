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
SETTINGS = buttons((('🔑 جلسة الديمو','edit:ssid'),('🌐 رابط الديمو','edit:url')),
                   (('💵 الأساس','edit:amount'),('🪜 مستويات التدبيل','edit:steps')),
                   (('🛟 تشغيل/إيقاف التعزيز','boost'),('➕ زيادة التعزيز','edit:increment')),
                   (('⏱ بيع آخر ثانية/بعد الإغلاق','early'),),
                   (('🛡 تشغيل/إيقاف الحدود','limits'),),
                   (('حد الربح','edit:profit'),('حد الخسارة','edit:loss')),BACK)
FIELDS = {'ssid':'أرسل إطار auth للديمو، أو UID متبوعاً بمسافة ثم SSID الخام.', 'url':'أرسل رابط WebSocket للديمو.',
          'amount':'أرسل مبلغ الأساس بالدولار (0.01–100000).', 'steps':'أرسل عدد المستويات من 0 إلى 4. القيمة 4 تعني أساس + 3 مضاعفات.',
          'increment':'أرسل الزيادة فوق الأساس بالدولار.', 'profit':'أرسل حد الربح بالدولار.', 'loss':'أرسل حد الخسارة بالدولار.'}


def settings_text(d):
    t,r,k=d['trading'],d['recovery'],d['risk']
    return (f"⚙️ M1 — تجريبي فقط\nالأساس: ${t['trade_amount']:g} | المستويات: {t['martingale_steps']}\n"
            f"التعزيز: {'مفعّل' if r['mode']=='ENABLED' else 'معطّل'} | +${r['increment']:g}\n"
            f"الدخول المبكر للبيع: {'مفعّل' if d['chess']['early'] else 'معطّل'}\n"
            f"الحدود: {'مفعّلة' if k['mode']=='LIMITED' else 'معطّلة'} | +${k['profit_limit']:g} / −${k['loss_limit']:g}\n"
            f"الجلسة: {'محفوظة' if d['demo_account']['ssid'] else 'غير مضافة'}")


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
        await msg.answer('♟ عبود أوتو تريدر | Abood Auto Trader\nM1 · حساب تجريبي',reply_markup=HOME)

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
                s=repo.doc['account_stats']['PRACTICE']
                await panel(cb,f"📊 ديمو\nالصفقات: {s['total_trades']} | ربح: {s['won_trades']} | خسارة: {s['lost_trades']} | تعادل: {s['draws']}\nسلاسل مستنفدة: {s['sequence_losses']}\nمبالغ الدخول: ${s['total_staked']:.2f} | العوائد: ${s['total_returned']:.2f}\nالصافي: ${s['net_pnl']:+.2f} | غير مؤكدة: {s['unconfirmed']}")
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
                    await panel(cb,'انتهت مراجعة الأمر المعلّق. لا يبدأ التداول تلقائياً.')
            elif action=='settings':
                await panel(cb,settings_text(repo.doc),SETTINGS)
            elif action in ('boost','early','limits'):
                d=repo.doc
                changes={'boost':{'recovery.mode':'TRADITIONAL' if d['recovery']['mode']=='ENABLED' else 'ENABLED'},
                         'early':{'chess.early':not d['chess']['early']},
                         'limits':{'risk.mode':'TRADITIONAL' if d['risk']['mode']=='LIMITED' else 'LIMITED'}}
                await repo.update(changes[action])
                await panel(cb,settings_text(repo.doc),SETTINGS)
            elif action.startswith('edit:'):
                key=action[5:]
                if key in FIELDS:
                    await state.set_state(Input.value)
                    await state.update_data(key=key)
                    await panel(cb,FIELDS[key]+'\n/cancel للإلغاء',buttons((('⬅️ الإعدادات','settings'),)))
        except ValueError:
            await panel(cb,'الإعداد غير صالح أو خارج النطاق.',SETTINGS)
        except Exception as exc:
            await panel(cb,'تعذّرت العملية: '+type(exc).__name__)

    @dp.message(Input.value)
    async def update(msg,state):
        key=(await state.get_data()).get('key')
        raw=(msg.text or '').strip()
        try:
            if key=='ssid':
                if len(raw)>20000:
                    raise ValueError
                if raw.startswith('42['):
                    if flag(json.loads(raw[2:])[1].get('isDemo')) is not True:
                        raise ValueError
                    frame=auth_frame(raw,'PRACTICE')
                else:
                    uid, ssid = raw.split(maxsplit=1)
                    if not uid.isascii() or not uid.isdecimal() or ssid.startswith('42'):
                        raise ValueError
                    frame=auth_frame(ssid,'PRACTICE',int(uid))
                if worker.task and not worker.task.done() or worker.pending:
                    raise ValueError
                await repo.update({'demo_account.ssid':frame})
            elif key=='url':
                if worker.task and not worker.task.done() or worker.pending:
                    raise ValueError
                await repo.update({'demo_account.ws_url':valid_url(raw)})
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
        await msg.answer(settings_text(repo.doc),reply_markup=SETTINGS)
    return dp
