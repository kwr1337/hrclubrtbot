import asyncio
import logging
import os
import re
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta

from aiogram import Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    BotCommand,
    BotCommandScopeDefault,
    BotCommandScopeChat,
)
from aiogram.client.bot import Bot, DefaultBotProperties
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters.state import StateFilter
from aiogram.utils.keyboard import InlineKeyboardBuilder

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import declarative_base, sessionmaker

from telethon import TelegramClient


BOT_TOKEN = "1282162158:AAHDrDTUAvDecZ-UehaoFdG6MkHxaKH1wvQ"
ROOT_ADMIN_ID = 137169162 
PRIVATE_GROUP_ID = -1001363051229

TELETHON_API_ID = "24732270"
TELETHON_API_HASH = "0e4e8581f1256800d859f7e9490b69d6"
TELETHON_SESSION = os.path.join(os.getcwd(), "user_session.session")

engine = create_engine("sqlite:///example7.db", echo=False)
Base = declarative_base()
SessionLocal = sessionmaker(bind=engine)

telethon_client = TelegramClient(TELETHON_SESSION, TELETHON_API_ID, TELETHON_API_HASH)


@contextmanager
def get_db():
    """Контекстный менеджер для работы с сессией SQLAlchemy."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Добавим функцию проверки рабочего времени (добавьте в начало файла, после импортов)
def is_work_time():
    now = datetime.now()
    is_weekday = now.weekday() < 5  # 0-4 это пн-пт
    is_work_hours = 8 <= now.hour < 20
    return is_weekday and is_work_hours


# -------------------------------------------------------
# МОДЕЛИ БД
# -------------------------------------------------------
class AdminUser(Base):
    __tablename__ = "admin_users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(Integer, unique=True, nullable=False)
    full_name = Column(String, nullable=True)


class GroupRules(Base):
    __tablename__ = "group_rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    text = Column(Text, nullable=False)


class UserRequest(Base):
    __tablename__ = "user_requests"

    id = Column(Integer, primary_key=True, autoincrement=True)
    chat_id = Column(Integer, nullable=False)
    third_party_chat_id = Column(Integer, nullable=True)
    person_type = Column(String, nullable=True)
    full_name = Column(String, nullable=True)
    phone = Column(String, nullable=True)
    workplace = Column(String, nullable=True)
    position = Column(String, nullable=True)
    username = Column(String, nullable=True)
    status = Column(String, default="pending")
    rejection_reason = Column(String, nullable=True)
    confirmation_code = Column(String, nullable=True)
    created_at = Column(String, nullable=True)  # Дата создания
    approved_at = Column(String, nullable=True)  # Дата одобрения
    rejected_at = Column(String, nullable=True)  # Дата отклонения
    rules_accepted_at = Column(String, nullable=True)  # Дата принятия правил

    approved_by = Column(Integer, nullable=True)
    rejected_by = Column(Integer, nullable=True)


class PendingInvite(Base):
    __tablename__ = "pending_invites"

    id = Column(Integer, primary_key=True, autoincrement=True)
    request_id = Column(Integer, nullable=False)  # ID заявки
    chat_id = Column(Integer, nullable=False)     # ID чата для отправки
    created_at = Column(String, nullable=False)   # Когда создан запрос
    is_third_party = Column(Integer, default=0)   # Флаг третьего лица
    confirmation_code = Column(String, nullable=True)  # Код для third_party


# Добавляем новую модель для хранения отложенных уведомлений о входе
class PendingJoinNotification(Base):
    __tablename__ = "pending_join_notifications"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)  # ID пользователя, который вошел
    chat_id = Column(Integer, nullable=False)  # ID группы
    full_name = Column(String, nullable=True)  # Имя пользователя
    workplace = Column(String, nullable=True)  # Место работы
    position = Column(String, nullable=True)   # Должность
    created_at = Column(String, nullable=False)  # Когда пользователь вошел


# Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

# Добавляем root админа
with get_db() as db:
    root_admin = db.query(AdminUser).filter_by(telegram_id=ROOT_ADMIN_ID).first()
    if not root_admin:
        new_root = AdminUser(telegram_id=ROOT_ADMIN_ID, full_name="Root Admin")
        db.add(new_root)
        db.commit()
        logging.info(f"Added root admin with ID {ROOT_ADMIN_ID}")


# -------------------------------------------------------
# СОСТОЯНИЯ (FSM)
# -------------------------------------------------------
class RequestFSM(StatesGroup):
    Choice = State()
    FullName = State()
    Phone = State()
    Workplace = State()
    Position = State()
    Username = State()
    Confirm = State()
    RejectionReason = State()


class AddAdminFSM(StatesGroup):
    WaitingFullName = State()
    WaitingID = State()


class RulesFSM(StatesGroup):
    WaitingText = State()


class DeleteAdminFSM(StatesGroup):
    Listing = State()


# -------------------------------------------------------
# УСТАНОВКА КОМАНД
# -------------------------------------------------------
async def set_bot_commands(bot: Bot):
    # --- Команды для обычных пользователей (в личных сообщениях) ---
    user_commands = [
        BotCommand(command="new", description="Создать новую заявку"),
        BotCommand(command="start", description="Начать работу"),
    ]
    await bot.set_my_commands(user_commands, scope=BotCommandScopeDefault())

    # --- Убираем все команды из групп ---
    await bot.set_my_commands([], scope=BotCommandScopeChat(chat_id=PRIVATE_GROUP_ID))

    with get_db() as db:
        all_admins = db.query(AdminUser).all()
        root_in_db = False
        for adm in all_admins:
            if adm.telegram_id == ROOT_ADMIN_ID:
                root_in_db = True
                break
        if not root_in_db:
            new_root = AdminUser(telegram_id=ROOT_ADMIN_ID, full_name="Root Admin")
            db.add(new_root)
            db.commit()
            all_admins.append(new_root)

        # Формируем команды для root-админа
        root_admin_commands = [
            BotCommand(command="addadmin", description="Добавить админа"),
            BotCommand(command="deladmin", description="Удалить админа"),
            BotCommand(command="setrules", description="Изменить правило группы"),
            BotCommand(command="check", description="Проверить заявки"),
            BotCommand(command="approved", description="Показать одобренные"),
            BotCommand(command="rejected", description="Показать отклонённые"),
            BotCommand(command="stats", description="Статистика"),
            BotCommand(command="help", description="Помощь"),
        ]

        # Формируем команды для обычных админов (без addadmin/deladmin)
        normal_admin_commands = [
            BotCommand(command="check", description="Проверить заявки"),
            BotCommand(command="setrules", description="Изменить правило группы"),
            BotCommand(command="approved", description="Показать одобренные"),
            BotCommand(command="rejected", description="Показать отклонённые"),
            BotCommand(command="stats", description="Статистика"),
            BotCommand(command="help", description="Помощь"),
        ]

        # Проставляем каждому админу нужный набор
        for adm in all_admins:
            if adm.telegram_id == ROOT_ADMIN_ID:
                await bot.set_my_commands(
                    root_admin_commands,
                    scope=BotCommandScopeChat(chat_id=adm.telegram_id)
                )
            else:
                await bot.set_my_commands(
                    normal_admin_commands,
                    scope=BotCommandScopeChat(chat_id=adm.telegram_id)
                )


# -------------------------------------------------------
# TELETHON АВТОРИЗАЦИЯ
# -------------------------------------------------------
async def authorize_user():
    """Авторизация Telethon (если сессия не создана — запросит телефон и код)."""
    if not telethon_client.is_connected():
        await telethon_client.connect()
    try:
        if await telethon_client.is_user_authorized():
            print("Telethon: пользователь уже авторизован.")
        else:
            print("Авторизация в Telethon...")
            phone = input("Введите номер телефона: ")
            sent_code = await telethon_client.send_code_request(phone)
            code = input("Код из Telegram: ")
            await telethon_client.sign_in(phone, code)
            telethon_client.session.save()
            print("Telethon: авторизация завершена.")
    except Exception as e:
        print(f"Ошибка авторизации Telethon: {e}")
        await telethon_client.disconnect()
        raise

    if not await telethon_client.is_user_authorized():
        print("Ошибка: Telethon клиент не авторизован.")
        await telethon_client.disconnect()
        raise RuntimeError("Telethon авторизация не удалась.")
    else:
        print("Telethon успешно авторизован.")


# -------------------------------------------------------
# ГЛАВНАЯ ЛОГИКА
# -------------------------------------------------------
async def check_pending_requests(bot: Bot):
    while True:
        # Ждем до 9:00 следующего рабочего дня
        now = datetime.now()
        next_run = now.replace(hour=2, minute=40, second=0, microsecond=0)
        if now >= next_run:
            next_run = next_run.replace(day=next_run.day + 1)
        
        # Пропускаем выходные
        while next_run.weekday() in [5, 6]:  # 5=суббота, 6=воскресенье
            next_run = next_run.replace(day=next_run.day + 1)
        
        delay = (next_run - now).total_seconds()
        await asyncio.sleep(delay)
        
        # Проверяем заявки
        with get_db() as db:
            # Получаем все pending заявки
            pending_requests = db.query(UserRequest).filter_by(status="pending").all()
            day_ago = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
            
            old_requests = [req for req in pending_requests 
                          if req.created_at and req.created_at <= day_ago]
            
            if old_requests:
                # Получаем список админов
                admins = db.query(AdminUser).all()
                
                # Формируем текст уведомления
                notification = (
                    "⚠️ <b>Напоминание о необработанных заявках</b>\n\n"
                    "Следующие заявки ожидают рассмотрения более 24 часов:\n\n"
                )
                
                for req in old_requests:
                    notification += (
                        f"• Заявка #{req.id} от {req.created_at}\n"
                        f"  ФИО: {req.full_name}\n"
                        f"  Тип: {'Своя' if req.person_type == 'self' else 'Третье лицо'}\n\n"
                    )
                
                notification += "Используйте /check для просмотра заявок."
                
                # Отправляем уведомление каждому админу
                for admin in admins:
                    try:
                        await bot.send_message(
                            chat_id=admin.telegram_id,
                            text=notification,
                            parse_mode="HTML"
                        )
                    except Exception as e:
                        logging.error(f"Ошибка отправки напоминания админу {admin.telegram_id}: {e}")

async def check_pending_invites(bot: Bot):
    while True:
        # Проверяем каждые 5 минут
        await asyncio.sleep(300)
        
        # Если не рабочее время, пропускаем проверку
        if not is_work_time():
            continue
            
        with get_db() as db:
            pending_invites = db.query(PendingInvite).all()
            
            for invite in pending_invites:
                try:
                    # Создаем ссылку-приглашение
                    link = await bot.create_chat_invite_link(
                        PRIVATE_GROUP_ID,
                        member_limit=1
                    )
                    
                    # Получаем данные заявки
                    req = db.query(UserRequest).filter_by(id=invite.request_id).first()
                    if not req:
                        # Если заявка не найдена, удаляем отложенное приглашение
                        db.delete(invite)
                        db.commit()
                        continue
                    
                    # Отправляем ссылку
                    await bot.send_message(
                        chat_id=invite.chat_id,
                        text=(
                            "🎉 <b>Добрый день!</b>\n\n"
                            "Вы ранее приняли правила группы в нерабочее время.\n"
                            f"Вот ваша ссылка для вступления в группу: {link.invite_link}"
                        ),
                        parse_mode="HTML"
                    )
                    
                    # Уведомляем админов
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    admins = db.query(AdminUser).all()
                    for admin in admins:
                        try:
                            await bot.send_message(
                                chat_id=admin.telegram_id,
                                text=f"✅ Пользователь {req.full_name} (заявка #{req.id}) получил отложенную ссылку на группу.\n📅 Дата: {current_time}"
                            )
                        except Exception as e:
                            logging.error(f"Ошибка отправки уведомления админу {admin.telegram_id}: {e}")
                    
                    # Удаляем запись из отложенных
                    db.delete(invite)
                    db.commit()
                    
                except Exception as e:
                    logging.error(f"Ошибка при отправке отложенной ссылки: {e}")

async def check_pending_join_notifications(bot: Bot):
    while True:
        # Проверяем каждые 5 минут
        await asyncio.sleep(300)
        
        # Если не рабочее время, пропускаем проверку
        if not is_work_time():
            continue
            
        with get_db() as db:
            pending_notifications = db.query(PendingJoinNotification).all()
            
            for notification in pending_notifications:
                try:
                    # Отправляем уведомление в группу
                    await bot.send_message(
                        chat_id=notification.chat_id,
                        text=(
                            f"👋 Добро пожаловать, {notification.full_name}!\n"
                            f"🏢 Место работы: {notification.workplace}\n"
                            f"💼 Должность: {notification.position}\n"
                        )
                    )
                    
                    # Удаляем запись из отложенных
                    db.delete(notification)
                    db.commit()
                    
                except Exception as e:
                    logging.error(f"Ошибка при отправке отложенного уведомления о входе: {e}")

async def main():
    logging.basicConfig(level=logging.INFO)
    await authorize_user()

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher(storage=MemoryStorage())
    
    # Устанавливаем команды
    await set_bot_commands(bot)
    
    # Запускаем проверку pending заявок в отдельной таске
    asyncio.create_task(check_pending_requests(bot))
    
    # Запускаем проверку отложенных ссылок в отдельной таске
    asyncio.create_task(check_pending_invites(bot))
    
    # Запускаем проверку отложенных уведомлений о входе в отдельной таске
    asyncio.create_task(check_pending_join_notifications(bot))

    # Функция проверки, является ли пользователь админом
    def check_is_admin(user_id: int) -> bool:
        with get_db() as db:
            adm = db.query(AdminUser).filter_by(telegram_id=user_id).first()
            return bool(adm)

    # Функция проверки, является ли пользователь корневым админом
    def is_root_admin(user_id: int) -> bool:
        return user_id == ROOT_ADMIN_ID

    # Генерация инлайн-клавиатуры «Назад / Отмена»
    def get_back_cancel_kb(back_callback: str) -> InlineKeyboardBuilder:
        kb = InlineKeyboardBuilder()
        kb.button(text="← Назад", callback_data=back_callback)
        kb.button(text="× Отмена", callback_data="cancel")
        kb.adjust(2)
        return kb

    # Показ подтверждения заявки
    async def show_confirmation(message: Message, state: FSMContext):
        data = await state.get_data()
        full_name = data.get("full_name", "—")
        phone = data.get("phone", "—")
        workplace = data.get("workplace", "—")
        position = data.get("position", "—")
        username = data.get("username", "—") if data.get("person_type") == "third_party" else "—"

        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Подтвердить", callback_data="confirm_yes")
        kb.button(text="✏️ Исправить", callback_data="edit_data")
        kb.adjust(2)

        text = (
            "Проверьте введённые данные:\n\n"
            f"📝 <b>Тип:</b> {'За себя' if data.get('person_type') == 'self' else 'За третье лицо'}\n"
            f"👤 <b>ФИО:</b> {full_name}\n"
            f"📞 <b>Телефон:</b> <code>{phone}</code>\n"
            f"🏢 <b>Место работы:</b> {workplace}\n"
            f"💼 <b>Должность:</b> {position}\n"
        )
        if data.get("person_type") == "third_party":
            text += f"👥 <b>Username:</b> {username}\n"

        await message.answer(text, reply_markup=kb.as_markup())


    # Применяем декоратор к командам
    @dp.message(Command("start"))
    async def cmd_start(message: Message, state: FSMContext):
        await state.clear()
        await message.answer(
            "Привет! Я бот для заявок.\n\n"
            "Чтобы подать новую заявку, введите /new.\n"
        )

    @dp.message(Command("new"))
    async def cmd_new(message: Message, state: FSMContext):
        await state.clear()
        kb = InlineKeyboardBuilder()
        kb.button(text="С (Свои данные)", callback_data="person_self")
        kb.button(text="ТП (Третье лицо)", callback_data="person_third")
        kb.adjust(1)
        await message.answer("Кого вы хотите добавить в заявку?", reply_markup=kb.as_markup())
        await state.set_state(RequestFSM.Choice)

    @dp.callback_query(F.data.in_({"person_self", "person_third"}), RequestFSM.Choice)
    async def person_choice(callback: CallbackQuery, state: FSMContext):
        p_type = "self" if callback.data == "person_self" else "third_party"
        await state.update_data(person_type=p_type)
        await state.set_state(RequestFSM.FullName)
        await callback.message.edit_text(
            "Введите ФИО (только русские буквы и пробелы). После ввода нажмите Enter:",
            reply_markup=get_back_cancel_kb("back_to_choice").as_markup()
        )

    @dp.callback_query(F.data == "back_to_choice", RequestFSM.FullName)
    async def back_to_choice_cb(callback: CallbackQuery, state: FSMContext):
        await state.set_state(RequestFSM.Choice)
        kb = InlineKeyboardBuilder()
        kb.button(text="С (Свои данные)", callback_data="person_self")
        kb.button(text="ТП (Третье лицо)", callback_data="person_third")
        kb.adjust(1)
        await callback.message.edit_text("Кого вы хотите добавить в заявку?", reply_markup=kb.as_markup())

    @dp.callback_query(F.data == "cancel")
    async def cancel_request(callback: CallbackQuery, state: FSMContext):
        await state.clear()
        await callback.message.edit_text("Заявка отменена пользователем.")


    # ---- Ввод ФИО ----
    @dp.message(RequestFSM.FullName)
    async def enter_fullname(message: Message, state: FSMContext):
        fio = message.text.strip()
        pattern = r"^[А-Яа-яЁё\s]+$"
        if not re.match(pattern, fio):
            await message.answer("ФИО должно содержать только русские буквы (и пробелы). Повторите ввод.")
            return

        data = await state.get_data()
        is_editing = data.get("editing", False)
        await state.update_data(full_name=fio)

        if is_editing:
            await state.update_data(editing=False)
            await state.set_state(RequestFSM.Confirm)
            await show_confirmation(message, state)
        else:
            await state.set_state(RequestFSM.Phone)
            await message.answer(
                "Введите номер телефона (начинается с 7, всего 11 цифр). После ввода нажмите Enter:",
                reply_markup=get_back_cancel_kb("back_to_choice").as_markup()
            )

    # ---- Ввод телефона ----
    @dp.message(RequestFSM.Phone)
    async def enter_phone(message: Message, state: FSMContext):
        phone = message.text.strip()
        if not (phone.startswith("7") and len(phone) == 11 and phone.isdigit()):
            await message.answer("Телефон должен начинаться с '7' и содержать ровно 11 цифр. Попробуйте снова.")
            return

        data = await state.get_data()
        is_editing = data.get("editing", False)
        await state.update_data(phone=phone)

        if is_editing:
            await state.update_data(editing=False)
            await state.set_state(RequestFSM.Confirm)
            await show_confirmation(message, state)
        else:
            await state.set_state(RequestFSM.Workplace)
            await message.answer(
                "Введите место работы (Юр.название компании)"
                    "(после ввода нажмите Enter):",
                reply_markup=get_back_cancel_kb("back_to_choice").as_markup()
            )

    # ---- Ввод места работы ----
    @dp.message(RequestFSM.Workplace)
    async def enter_workplace(message: Message, state: FSMContext):
        place = message.text.strip()
        data = await state.get_data()
        is_editing = data.get("editing", False)
        await state.update_data(workplace=place)

        if is_editing:
            await state.update_data(editing=False)
            await state.set_state(RequestFSM.Confirm)
            await show_confirmation(message, state)
        else:
            # После места работы → спрашиваем должность
            await state.set_state(RequestFSM.Position)
            await message.answer(
                "Введите должность (после ввода нажмите Enter):",
                reply_markup=get_back_cancel_kb("back_to_workplace").as_markup()
            )

    @dp.message(RequestFSM.Position)
    async def enter_position(message: Message, state: FSMContext):
        pos = message.text.strip()
        data = await state.get_data()
        is_editing = data.get("editing", False)
        await state.update_data(position=pos)

        # если редактировали
        if is_editing:
            await state.update_data(editing=False)
            await state.set_state(RequestFSM.Confirm)
            await show_confirmation(message, state)
        else:
            # если third_party → запрашиваем username, иначе → подтверждение
            if data.get("person_type") == "third_party":
                await state.set_state(RequestFSM.Username)
                await message.answer(
                    "Введите username третьего лица (например, @username):",
                    reply_markup=get_back_cancel_kb("back_to_position").as_markup()
                )
            else:
                await state.set_state(RequestFSM.Confirm)
                await show_confirmation(message, state)

    @dp.callback_query(F.data == "back_to_workplace", RequestFSM.Position)
    async def back_to_workplace_cb(callback: CallbackQuery, state: FSMContext):
        await state.set_state(RequestFSM.Workplace)
        await callback.message.edit_text(
            "Введите место работы (после ввода нажмите Enter):",
            reply_markup=get_back_cancel_kb("back_to_choice").as_markup()
        )

    @dp.callback_query(F.data == "back_to_position", RequestFSM.Username)
    async def back_to_position_cb(callback: CallbackQuery, state: FSMContext):
        await state.set_state(RequestFSM.Position)
        await callback.message.edit_text(
            "Введите должность (после ввода нажмите Enter):",
            reply_markup=get_back_cancel_kb("back_to_workplace").as_markup()
        )

    # ---- Ввод username (третье лицо) ----
    @dp.message(RequestFSM.Username)
    async def enter_username(message: Message, state: FSMContext):
        data = await state.get_data()
        is_editing = data.get("editing", False)
        usern = message.text.strip()
        await state.update_data(username=usern)

        if is_editing:
            await state.update_data(editing=False)
            await state.set_state(RequestFSM.Confirm)
            await show_confirmation(message, state)
        else:
            await state.set_state(RequestFSM.Confirm)
            await show_confirmation(message, state)

    # ---- Подтверждение заявки (запись в БД) ----
    @dp.callback_query(F.data == "confirm_yes", RequestFSM.Confirm)
    async def confirm_request(callback: CallbackQuery, state: FSMContext):
        data = await state.get_data()
        try:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Получаем username пользователя или ставим прочерк
            username = "—"
            if data["person_type"] == "self":
                username = callback.from_user.username or "—"
            
            with get_db() as db:
                new_req = UserRequest(
                    chat_id=callback.from_user.id,
                    person_type=data["person_type"],
                    full_name=data["full_name"],
                    phone=data["phone"],
                    workplace=data["workplace"],
                    position=data.get("position"),
                    username=username if data["person_type"] == "self" else data.get("username"),
                    status="pending",
                    created_at=current_time
                )
                db.add(new_req)
                db.commit()

                # Уведомление админов с кнопками
                admins = db.query(AdminUser).all()
                for admin in admins:
                    try:
                        kb = InlineKeyboardBuilder()
                        kb.button(text="✅ Одобрить", callback_data=f"approve_{new_req.id}")
                        kb.button(text="❌ Отклонить", callback_data=f"reject_{new_req.id}")
                        kb.adjust(2)
                        
                        await callback.message.bot.send_message(
                            chat_id=admin.telegram_id,
                            text=(
                                f"🆕 <b>Новая заявка</b> #{new_req.id}\n\n"
                                f"📝 <b>Тип:</b> {'Своя' if new_req.person_type == 'self' else 'Третье лицо'}\n"
                                f"👤 <b>ФИО:</b> {new_req.full_name}\n"
                                f"📞 <b>Телефон:</b> <code>{new_req.phone}</code>\n"
                                f"🏢 <b>Место работы:</b> {new_req.workplace}\n"
                                f"💼 <b>Должность:</b> {new_req.position}\n"
                                f"👥 <b>Username:</b> {new_req.username if new_req.username else '—'}\n"
                                f"📅 <b>Дата создания:</b> {current_time}\n\n"
                                "⚠️ <i>Не забудьте проверить заявку!</i>"
                            ),
                            reply_markup=kb.as_markup(),
                            parse_mode="HTML"
                        )
                    except Exception as e:
                        logging.error(f"Ошибка отправки уведомления админу {admin.telegram_id}: {e}")

            await state.clear()
            await callback.message.edit_text(
                text=(
                    "✅ <b>Ваша заявка успешно отправлена!</b>\n\n"
                    "📋 <b>Детали заявки:</b>\n"
                    f"👤 <b>ФИО:</b> {data['full_name']}\n"
                    f"📞 <b>Телефон:</b> <code>{data['phone']}</code>\n"
                    f"🏢 <b>Место работы:</b> {data['workplace']}\n"
                    f"💼 <b>Должность:</b> {data['position']}\n"
                    f"👥 <b>Username:</b> {callback.from_user.username or '—' if data['person_type'] == 'self' else data.get('username', '—')}\n\n"
                    "⏳ <i>Ожидайте решения администратора.</i>"
                ),
                parse_mode="HTML"
            )
            await callback.answer("Заявка успешно подтверждена.", show_alert=False)
        except Exception as e:
            logging.error(f"Ошибка при сохранении заявки: {e}")
            await callback.answer("Ошибка при сохранении заявки.", show_alert=True)

    # ---- «Исправить» ----
    @dp.callback_query(F.data == "edit_data", RequestFSM.Confirm)
    async def edit_data(callback: CallbackQuery, state: FSMContext):
        data = await state.get_data()
        kb = InlineKeyboardBuilder()
        kb.button(text="ФИО", callback_data="edit_fullname")
        kb.button(text="Телефон", callback_data="edit_phone")
        kb.button(text="Место работы", callback_data="edit_workplace")
        kb.button(text="Должность", callback_data="edit_position")  # <--- добавили
        if data.get("person_type") == "third_party":
            kb.button(text="Username", callback_data="edit_username")
        kb.adjust(2)

        await callback.message.edit_text("Выберите поле для исправления:", reply_markup=kb.as_markup())
        await callback.answer()

    @dp.callback_query(F.data.startswith("edit_"), RequestFSM.Confirm)
    async def edit_field(callback: CallbackQuery, state: FSMContext):
        field = callback.data.split("_")[1]
        field_map = {
            "fullname": ("Введите новое ФИО:", RequestFSM.FullName),
            "phone": ("Введите новый номер телефона:", RequestFSM.Phone),
            "workplace": ("Введите новое место работы:", RequestFSM.Workplace),
            "position": ("Введите новую должность:", RequestFSM.Position),  # <--- добавили
            "username": ("Введите новый username (например, @username):", RequestFSM.Username),
        }
        if field in field_map:
            text, next_state = field_map[field]
            await state.update_data(editing=True)
            await state.set_state(next_state)
            await callback.message.edit_text(
                text,
                reply_markup=get_back_cancel_kb("back_to_confirmation").as_markup()
            )
            await callback.answer()

    @dp.callback_query(F.data == "back_to_confirmation")
    async def back_to_confirmation_cb(callback: CallbackQuery, state: FSMContext):
        await state.update_data(editing=False)
        await state.set_state(RequestFSM.Confirm)
        await show_confirmation(callback.message, state)
        await callback.answer()

    # -------------------------------------------------------
    #               Админские команды
    # -------------------------------------------------------
    @dp.message(Command("setrules"))
    async def cmd_setrules(message: Message, state: FSMContext):
        if not check_is_admin(message.from_user.id):
            await message.answer("У вас нет прав для изменения правил.")
            return

        # Смотрим, какие правила сейчас
        with get_db() as db:
            current_rules = db.query(GroupRules).first()
            current_text = current_rules.text if current_rules else "Правил пока нет."

        # Кнопка «Отмена»
        kb = InlineKeyboardBuilder()
        kb.button(text="× Отмена", callback_data="cancel_setrules")
        kb.adjust(1)

        await message.answer(
            f"Текущий текст правил:\n\n"
            f"{current_text}\n\n"
            "Введите новый текст правил (или нажмите «Отмена»):",
            reply_markup=kb.as_markup()
        )
        await state.set_state(RulesFSM.WaitingText)

    @dp.callback_query(F.data == "cancel_setrules", RulesFSM.WaitingText)
    async def cancel_setrules_cb(callback: CallbackQuery, state: FSMContext):
        await state.clear()
        await callback.message.edit_text("Изменение правил отменено.")
        await callback.answer()

    @dp.message(StateFilter(RulesFSM.WaitingText))
    async def rules_waiting_text(message: Message, state: FSMContext):
        new_text = message.text.strip()
        with get_db() as db:
            rules_obj = db.query(GroupRules).first()
            if not rules_obj:
                rules_obj = GroupRules(text=new_text)
                db.add(rules_obj)
            else:
                rules_obj.text = new_text
            db.commit()
        await message.answer("Правила группы обновлены.")
        await state.clear()

    # ---- Добавление админа (только root) ----
    @dp.message(Command("addadmin"))
    async def cmd_addadmin(message: Message, state: FSMContext):
        if message.from_user.id != ROOT_ADMIN_ID:
            await message.answer("Только главный админ может добавлять новых администраторов.")
            return

        kb = InlineKeyboardBuilder()
        kb.button(text="× Отмена", callback_data="cancel_addadmin")
        kb.adjust(1)

        await message.answer("Введите ФИО нового админа:", reply_markup=kb.as_markup())
        await state.set_state(AddAdminFSM.WaitingFullName)

    @dp.callback_query(F.data == "cancel_addadmin", AddAdminFSM.WaitingFullName)
    async def cancel_addadmin_fullname_cb(callback: CallbackQuery, state: FSMContext):
        await state.clear()
        await callback.message.edit_text("Добавление админа отменено.")
        await callback.answer()

    @dp.message(StateFilter(AddAdminFSM.WaitingFullName), F.text)
    async def addadmin_fullname(message: Message, state: FSMContext):
        fullname = message.text.strip()
        kb = InlineKeyboardBuilder()
        kb.button(text="× Отмена", callback_data="cancel_addadmin_id")
        kb.adjust(1)

        await state.update_data(admin_fullname=fullname)
        await message.answer(
            "Теперь введите Telegram ID нового админа (число):",
            reply_markup=kb.as_markup()
        )
        await state.set_state(AddAdminFSM.WaitingID)

    @dp.callback_query(F.data == "cancel_addadmin_id", AddAdminFSM.WaitingID)
    async def cancel_addadmin_id_cb(callback: CallbackQuery, state: FSMContext):
        await state.clear()
        await callback.message.edit_text("Добавление админа отменено (на стадии ID).")
        await callback.answer()

    @dp.message(StateFilter(AddAdminFSM.WaitingID))
    async def addadmin_id(message: Message, state: FSMContext):
        try:
            new_tid = int(message.text.strip())
        except:
            await message.answer("Некорректный ID (введите число).")
            return

        data = await state.get_data()
        admin_fullname = data.get("admin_fullname", "noname")

        with get_db() as db:
            exist = db.query(AdminUser).filter_by(telegram_id=new_tid).first()
            if exist:
                await message.answer("Такой админ уже есть.")
            else:
                new_admin = AdminUser(telegram_id=new_tid, full_name=admin_fullname)
                db.add(new_admin)
                db.commit()
                await message.answer(f"Админ {admin_fullname} (id={new_tid}) добавлен.")
        await state.clear()
        await set_bot_commands(bot)

    # ---- Удаление админа (только root) ----
    @dp.message(Command("deladmin"))
    async def cmd_deladmin(message: Message, state: FSMContext):
        """Команда для root-админа, чтобы удалить одного из текущих админов (кроме root)."""
        if message.from_user.id != ROOT_ADMIN_ID:
            await message.answer("Только главный админ может удалять администраторов.")
            return

        with get_db() as db:
            admins = db.query(AdminUser).filter(AdminUser.telegram_id != ROOT_ADMIN_ID).all()
            if not admins:
                await message.answer("Нет администраторов (кроме root), чтобы удалять.")
                return

            # Сохраним в состояние их id и начнём листать
            await state.update_data(
                deladmin_ids=[adm.id for adm in admins],
                deladmin_index=0
            )
        await show_admin_list_to_delete(message, state, edit=False)
        await state.set_state(DeleteAdminFSM.Listing)

    async def show_admin_list_to_delete(message: Message, state: FSMContext, edit: bool = True):
        """Показ одного админа из списка, с кнопкой 'Удалить' + листание."""
        data = await state.get_data()
        ids = data.get("deladmin_ids", [])
        idx = data.get("deladmin_index", 0)
        if not ids or idx >= len(ids):
            await message.answer("Все администраторы просмотрены или удалены.")
            return

        with get_db() as db:
            adm_user = db.get(AdminUser, ids[idx])
            if not adm_user:
                await message.answer("Админ не найден в БД (возможно, уже удалён).")
                return

        kb = InlineKeyboardBuilder()
        kb.button(text="Удалить", callback_data=f"deladm_{adm_user.id}")
        if idx > 0:
            kb.button(text="⬅️ Назад", callback_data="prev_deladmin")
        if idx < len(ids) - 1:
            kb.button(text="➡️ Далее", callback_data="next_deladmin")
        kb.adjust(2)

        text = (
            f"<b>Администратор</b>:\n\n"
            f"ID в таблице: {adm_user.id}\n"
            f"TelegramID: {adm_user.telegram_id}\n"
            f"ФИО: {adm_user.full_name or '—'}\n"
        )
        if edit:
            await message.edit_text(text, reply_markup=kb.as_markup())
        else:
            await message.answer(text, reply_markup=kb.as_markup())

    @dp.callback_query(F.data == "prev_deladmin", DeleteAdminFSM.Listing)
    async def prev_deladmin_cb(callback: CallbackQuery, state: FSMContext):
        data = await state.get_data()
        idx = data.get("deladmin_index", 0)
        if idx > 0:
            idx -= 1
        await state.update_data(deladmin_index=idx)
        await callback.answer()
        await show_admin_list_to_delete(callback.message, state)

    @dp.callback_query(F.data == "next_deladmin", DeleteAdminFSM.Listing)
    async def next_deladmin_cb(callback: CallbackQuery, state: FSMContext):
        data = await state.get_data()
        idx = data.get("deladmin_index", 0)
        idx += 1
        await state.update_data(deladmin_index=idx)
        await callback.answer()
        await show_admin_list_to_delete(callback.message, state)

    @dp.callback_query(F.data.startswith("deladm_"), DeleteAdminFSM.Listing)
    async def delete_admin_confirm(callback: CallbackQuery, state: FSMContext):
        # удаляем админа
        adm_id = int(callback.data.split("_")[1])  # это колонка "id" в таблице admin_users
        with get_db() as db:
            adm_user = db.get(AdminUser, adm_id)
            if not adm_user:
                await callback.answer("Админ не найден.", show_alert=True)
                return
            # Удаляем
            del_telegram_id = adm_user.telegram_id
            db.delete(adm_user)
            db.commit()

        await callback.answer("Админ удалён из БД.")
        await callback.message.edit_text(
            f"Админ c ID={adm_id} (tg_id={del_telegram_id}) удалён."
        )

        # Удаляем из списка в состоянии
        data = await state.get_data()
        ids = data.get("deladmin_ids", [])
        idx = data.get("deladmin_index", 0)

        if adm_id in ids:
            ids.remove(adm_id)
        # сдвигаем индекс, если надо
        if idx >= len(ids):
            idx = len(ids) - 1
        await state.update_data(deladmin_ids=ids, deladmin_index=idx)

        # Меняем команды у удалённого админа (теперь он не админ)
        try:
            # только юзерские команды
            user_commands = [
                BotCommand(command="start", description="Начать работу"),
                BotCommand(command="help", description="Помощь"),
                BotCommand(command="new", description="Создать новую заявку"),
            ]
            await callback.message.bot.set_my_commands(
                user_commands,
                scope=BotCommandScopeChat(chat_id=del_telegram_id)
            )
        except:
            pass  # возможно, user не общался с ботом

        # Переходим к следующему, если остался
        if idx >= 0 and idx < len(ids):
            await show_admin_list_to_delete(callback.message, state, edit=False)
        else:
            await callback.message.answer("Больше админов для удаления нет.")
            await state.clear()

    # ---- Статистика ----
    @dp.message(Command("stats"))
    async def admin_stats(message: Message):
        if not check_is_admin(message.from_user.id):
            await message.answer("У вас нет прав.")
            return

        with get_db() as db:
            total_req = db.query(UserRequest).count()
            pending_req = db.query(UserRequest).filter_by(status="pending").count()
            approved_req = db.query(UserRequest).filter_by(status="approved").count()
            rejected_req = db.query(UserRequest).filter_by(status="rejected").count()

        await message.answer(
            text=(
                "📊 <b>Статистика заявок</b>\n\n"
                f"📂 <b>Всего заявок:</b> {total_req}\n"
                f"⏳ <b>В ожидании:</b> {pending_req}\n"
                f"✅ <b>Одобрено:</b> {approved_req}\n"
                f"❌ <b>Отклонено:</b> {rejected_req}\n\n"
                "🛠 <i>Используйте команду /check для просмотра заявок.</i>"
            ),
            parse_mode="HTML"
        )

    @dp.message(Command("check"))
    async def admin_check(message: Message, state: FSMContext):
        if not check_is_admin(message.from_user.id):
            await message.answer("У вас нет прав.")
            return
        with get_db() as db:
            # Получаем заявки в обратном порядке
            pendings = db.query(UserRequest).filter_by(status="pending").order_by(UserRequest.id.desc()).all()
            if not pendings:
                await message.answer("Нет заявок со статусом 'pending'.")
                return
            await state.update_data(pending_ids=[req.id for req in pendings], current_index=0)
            await show_request_to_admin(message, state, edit=False)

    # Показ (одной) pending-заявки
    async def show_request_to_admin(message: Message, state: FSMContext, edit: bool = True):
        data = await state.get_data()
        p_ids = data.get("pending_ids", [])
        idx = data.get("current_index", 0)

        if not p_ids or idx >= len(p_ids):
            await message.answer("Все заявки обработаны.")
            return

        with get_db() as db:
            req = db.get(UserRequest, p_ids[idx])
        if not req:
            await message.answer("Заявка не найдена.")
            return

        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Одобрить", callback_data=f"approve_{req.id}")
        kb.button(text="❌ Отклонить", callback_data=f"reject_{req.id}")
        if idx > 0:
            kb.button(text="⬅️ Назад", callback_data="prev_request")
        if idx < len(p_ids) - 1:
            kb.button(text="➡️ Далее", callback_data="next_request")
        kb.adjust(2)

        text = (
            f"🆕 <b>Новая заявка</b> #{req.id}\n\n"
            f"📝 <b>Тип:</b> {'Своя' if req.person_type == 'self' else 'Третье лицо'}\n"
            f"👤 <b>ФИО:</b> {req.full_name}\n"
            f"📞 <b>Телефон:</b> <code>{req.phone}</code>\n"
            f"🏢 <b>Место работы:</b> {req.workplace}\n"
            f"💼 <b>Должность:</b> {req.position}\n"
            f"👥 <b>Username:</b> {req.username if req.username else '—'}\n"
            f"📅 <b>Дата создания:</b> {req.created_at}\n"
            f"Статус: {req.status}"
        )

        if edit:
            await message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="HTML")
        else:
            await message.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML")

    @dp.callback_query(F.data == "next_request")
    async def next_request_cb(callback: CallbackQuery, state: FSMContext):
        data = await state.get_data()
        idx = data.get("current_index", 0)
        await state.update_data(current_index=idx + 1)
        await callback.answer()
        await show_request_to_admin(callback.message, state)

    @dp.callback_query(F.data == "prev_request")
    async def prev_request_cb(callback: CallbackQuery, state: FSMContext):
        data = await state.get_data()
        idx = data.get("current_index", 0)
        await state.update_data(current_index=idx - 1)
        await callback.answer()
        await show_request_to_admin(callback.message, state)

    @dp.message(Command("approved"))
    async def admin_approved(message: Message, state: FSMContext):
        if not check_is_admin(message.from_user.id):
            await message.answer("У вас нет прав.")
            return
        with get_db() as db:
            app_list = db.query(UserRequest).filter_by(status="approved").all()
            if not app_list:
                await message.answer("Нет одобренных заявок.")
                return
            await state.update_data(approved_ids=[r.id for r in app_list], approved_index=0)
            await show_approved_request(message, state, edit=False)

    async def show_approved_request(message: Message, state: FSMContext, edit: bool = True):
        data = await state.get_data()
        a_ids = data.get("approved_ids", [])
        idx = data.get("approved_index", 0)

        if not a_ids or idx >= len(a_ids):
            await message.answer("Список одобренных заявок пуст.")
            return

        with get_db() as db:
            req = db.get(UserRequest, a_ids[idx])
        if not req:
            await message.answer("Заявка не найдена.")
            return

        kb = InlineKeyboardBuilder()
        if idx > 0:
            kb.button(text="⬅️ Назад", callback_data="prev_approved")
        if idx < len(a_ids) - 1:
            kb.button(text="➡️ Далее", callback_data="next_approved")
        kb.adjust(2)

        if req.approved_by:
            admin_user = db.query(AdminUser).filter_by(telegram_id=req.approved_by).first()
            admin_name = admin_user.full_name if admin_user else str(req.approved_by)
        else:
            admin_name = "—"

        text = (
            f"✅ <b>Принятая заявка</b> #{req.id}\n\n"
            f"📝 <b>Тип:</b> {'С (свои)' if req.person_type=='self' else 'ТП (третье лицо)'}\n"
            f"👤 <b>ФИО:</b> {req.full_name}\n"
            f"📞 <b>Телефон:</b> {req.phone}\n"
            f"🏢 <b>Место работы:</b> {req.workplace}\n"
            f"💼 <b>Должность:</b> {req.position}\n"
            f"👥 <b>Username:</b> {req.username if req.username else '—'}\n"
            f"📅 <b>Дата создания:</b> {req.created_at}\n"
            f"📅 <b>Дата одобрения:</b> {req.approved_at}\n"
            f"👤 <b>Одобрил:</b> {admin_name}\n"
            f"✅ <b>Правила приняты:</b> {req.rules_accepted_at or '—'}\n"
        )
        if edit:
            await message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="HTML")
        else:
            await message.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML")

    @dp.callback_query(F.data == "next_approved")
    async def next_approved_cb(callback: CallbackQuery, state: FSMContext):
        data = await state.get_data()
        idx = data.get("approved_index", 0)
        await state.update_data(approved_index=idx + 1)
        await callback.answer()
        await show_approved_request(callback.message, state)

    @dp.callback_query(F.data == "prev_approved")
    async def prev_approved_cb(callback: CallbackQuery, state: FSMContext):
        data = await state.get_data()
        idx = data.get("approved_index", 0)
        await state.update_data(approved_index=idx - 1)
        await callback.answer()
        await show_approved_request(callback.message, state)

    @dp.message(Command("rejected"))
    async def admin_rejected(message: Message, state: FSMContext):
        if not check_is_admin(message.from_user.id):
            await message.answer("У вас нет прав.")
            return
        with get_db() as db:
            rej_list = db.query(UserRequest).filter_by(status="rejected").all()
            if not rej_list:
                await message.answer("Нет отклонённых заявок.")
                return
            await state.update_data(rejected_ids=[r.id for r in rej_list], rejected_index=0)
            await show_rejected_request(message, state, edit=False)

    async def show_rejected_request(message: Message, state: FSMContext, edit: bool = True):
        data = await state.get_data()
        r_ids = data.get("rejected_ids", [])
        idx = data.get("rejected_index", 0)

        if not r_ids or idx >= len(r_ids):
            await message.answer("Список отклонённых заявок пуст.")
            return

        with get_db() as db:
            req = db.get(UserRequest, r_ids[idx])
        if not req:
            await message.answer("Заявка не найдена.")
            return

        kb = InlineKeyboardBuilder()
        if idx > 0:
            kb.button(text="⬅️ Назад", callback_data="prev_rejected")
        if idx < len(r_ids) - 1:
            kb.button(text="➡️ Далее", callback_data="next_rejected")
        kb.adjust(2)

        if req.rejected_by:
            admin_user = db.query(AdminUser).filter_by(telegram_id=req.rejected_by).first()
            admin_name = admin_user.full_name if admin_user else str(req.rejected_by)
        else:
            admin_name = "—"

        text = (
            f"❌ <b>Отклоненная заявка</b> #{req.id}\n\n"
            f"📝 <b>Тип:</b> {'С (свои)' if req.person_type=='self' else 'ТП (третье лицо)'}\n"
            f"👤 <b>ФИО:</b> {req.full_name}\n"
            f"📞 <b>Телефон:</b> {req.phone}\n"
            f"🏢 <b>Место работы:</b> {req.workplace}\n"
            f"💼 <b>Должность:</b> {req.position}\n"
            f"👥 <b>Username:</b> {req.username if req.username else '—'}\n"
            f"📅 <b>Дата создания:</b> {req.created_at}\n"
            f"📅 <b>Дата отклонения:</b> {req.rejected_at}\n"
            f"👤 <b>Отклонил:</b> {admin_name}\n"
            f"❌ <b>Причина отказа:</b> {req.rejection_reason or '—'}\n"
        )
        if edit:
            await message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="HTML")
        else:
            await message.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML")

    @dp.callback_query(F.data == "next_rejected")
    async def next_rejected_cb(callback: CallbackQuery, state: FSMContext):
        data = await state.get_data()
        idx = data.get("rejected_index", 0)
        await state.update_data(rejected_index=idx + 1)
        await callback.answer()
        await show_rejected_request(callback.message, state)

    @dp.callback_query(F.data == "prev_rejected")
    async def prev_rejected_cb(callback: CallbackQuery, state: FSMContext):
        data = await state.get_data()
        idx = data.get("rejected_index", 0)
        await state.update_data(rejected_index=idx - 1)
        await callback.answer()
        await show_rejected_request(callback.message, state)

    # ---- Одобрить заявку (approve_) ----
    @dp.callback_query(F.data.startswith("approve_"))
    async def approve_request(callback: CallbackQuery, state: FSMContext):
        admin_id = callback.from_user.id
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with get_db() as db:
            # Ищем админа, который нажал на кнопку
            admin_user = db.query(AdminUser).filter_by(telegram_id=admin_id).first()
            admin_name = admin_user.full_name if admin_user else str(admin_id)

            req_id = int(callback.data.split("_")[1])
            req = db.get(UserRequest, req_id)
            if not req or req.status in ["approved", "rejected"]:
                await callback.answer("Заявка не найдена или уже обработана!", show_alert=True)
                return

            # Помечаем заявку одобренной
            req.status = "approved"
            req.approved_by = admin_id
            req.approved_at = current_time

            # Генерируем код для третьего лица, если нужно
            if req.person_type == "third_party" and req.username:
                req.confirmation_code = str(uuid.uuid4())[:8]

            db.commit()

        # Уведомление админу об успешном одобрении
        await callback.message.bot.send_message(
            chat_id=admin_id,
            text=f"✅ Заявка #{req_id} успешно одобрена и правила отправлены пользователю."
        )

        # далее отправка правил
        with get_db() as db:
            rules_obj = db.query(GroupRules).first()
            rules_text = rules_obj.text if rules_obj else "Правила пока не заданы."

            req = db.get(UserRequest, req_id)
            c_id = req.chat_id
            p_type = req.person_type
            code = req.confirmation_code
            uname = req.username

        if p_type == "third_party" and uname:
            # Правила отправляем через Telethon в ЛС третьему лицу + инструкция
            # Редактируемый текст, например:
            text_for_third = (
                 "🎉 <b>Ваша заявка одобрена!</b>\n\n"
                 "📋 <b>Правила группы:</b>\n"
                  f"  <b>{rules_text}</b>\n\n"
                "✅ <b>Что бы принять или отклонить правила, отправьте команды боту @hrclubrtbot:</b>\n"
                "✅ <b>Принять правила:</b>\n"
                 f"Отправьте команду:  <code>/accept {code}</code>\n\n"
                 "❌ <b>Отклонить правила:</b>\n"
                f"Отправьте команду:  <code>/decline {code}</code>"
            )
            try:
                if not telethon_client.is_connected():
                    await telethon_client.connect()

                user = await telethon_client.get_entity(uname)
                await telethon_client.send_message(user, text_for_third, parse_mode='html')
                await callback.answer("Заявка одобрена. Правила отправлены третьему лицу (ЛС).")
            except Exception as e:
                logging.error(f"Ошибка при отправке третьему лицу {uname}: {e}")
                await callback.answer("Ошибка при отправке третьему лицу.", show_alert=True)
        else:
            # Если заявка «за себя» — отправляем правила в ЛС самому пользователю бота
            rules_text_formatted = (
                "🎉 <b>Ваша заявка одобрена!</b>\n\n"
                "📋 <b>Правила группы:</b>\n"
                f"   <b>{rules_text}</b>\n\n"
            )
            kb = InlineKeyboardBuilder()
            kb.button(text="✅ Принять", callback_data=f"accept_rules_{req_id}")
            kb.button(text="❌ Отклонить", callback_data=f"decline_rules_{req_id}")
            kb.adjust(2)

            try:
                # Для обычных пользователей (self):
                link = await callback.message.bot.create_chat_invite_link(
                    PRIVATE_GROUP_ID,
                    member_limit=1  # Ограничение на 1 использование
                )

                await callback.message.bot.send_message(
                    chat_id=c_id,
                    text=rules_text_formatted,
                    reply_markup=kb.as_markup()
                )
                await callback.answer("Заявка одобрена, правила отправлены.")
            except Exception as e:
                logging.error(f"Ошибка при отправке правил пользователю {c_id}: {e}")
                await callback.answer("Ошибка при отправке правил пользователю.", show_alert=True)

        # После обработки заявки удаляем её из списка pending
        data = await state.get_data()
        if data.get("pending_ids"):
            pending_ids = data["pending_ids"]
            if req_id in pending_ids:
                pending_ids.remove(req_id)
                current_index = data.get("current_index", 0)
                if current_index >= len(pending_ids):
                    current_index = max(0, len(pending_ids) - 1)
                await state.update_data(pending_ids=pending_ids, current_index=current_index)
                
                if pending_ids:
                    await show_request_to_admin(callback.message, state)
                else:
                    await callback.message.edit_text("Все заявки обработаны.")

    # ---- Отклонить заявку (reject_) ----
    @dp.callback_query(F.data.startswith("reject_"))
    async def reject_request_cb(callback: CallbackQuery, state: FSMContext):
        admin_id = callback.from_user.id
        if not check_is_admin(admin_id):
            await callback.answer("Вы не админ!", show_alert=True)
            return

        req_id = int(callback.data.split("_")[1])
        with get_db() as db:
            req = db.get(UserRequest, req_id)
            if not req:
                await callback.answer("Заявка не найдена.", show_alert=True)
                return
            if req.status in ["approved", "rejected"]:
                await callback.answer("Уже обработана!", show_alert=True)
                return

        await state.update_data(rejecting_id=req_id)
        await callback.message.answer("Укажите причину отказа:")
        await state.set_state(RequestFSM.RejectionReason)

    @dp.message(StateFilter(RequestFSM.RejectionReason))
    async def save_rejection_reason(message: Message, state: FSMContext):
        data = await state.get_data()
        req_id = data.get("rejecting_id")
        reason = message.text.strip()
        admin_id = message.from_user.id

        if req_id:
            with get_db() as db:
                req = db.get(UserRequest, req_id)
                if not req:
                    await message.answer("Заявка не найдена.")
                    await state.clear()
                    return
                if req.status in ["approved", "rejected"]:
                    await message.answer("Эта заявка уже обработана другим админом.")
                    await state.clear()
                    return

                # Отмечаем заявку как отклонённую
                req.status = "rejected"
                req.rejection_reason = reason
                req.rejected_by = admin_id
                db.commit()

                c_id = req.chat_id

            # Уведомляем самого пользователя об отказе
            await message.bot.send_message(
                chat_id=c_id,
                text=(
                    "❌ <b>Ваша заявка отклонена.</b>\n\n"
                    f"📝 <b>Причина:</b> <i>{reason}</i>\n\n"
                    "�� <b>Что делать дальше?</b>\n"
                    "Вы можете подать новую заявку, исправив указанные ошибки.\n"
                    "Для этого используйте команду /new."
                ),
                parse_mode="HTML"
            )
            await message.answer("Заявка отклонена. Причина сохранена.")

            # ===== Добавляем уведомление другим админам =====
            # Сначала найдём имя текущего админа (кто нажал «Отклонить»)
            with get_db() as db:
                admin_user = db.query(AdminUser).filter_by(telegram_id=admin_id).first()
                admin_name = admin_user.full_name if admin_user else str(admin_id)

                all_adms = db.query(AdminUser).all()
                for a in all_adms:
                    if a.telegram_id != admin_id:
                        try:
                            await message.bot.send_message(
                                chat_id=a.telegram_id,
                                text=(
                                    f"Заявка #{req_id} отклонена админом {admin_name}. "
                                    f"Причина: {reason}"
                                )
                            )
                        except Exception as e:
                            logging.warning(f"Не удалось уведомить админа {a.telegram_id}: {e}")
            # ================================================

            # Убираем заявку из списка «pending» (если нужно листать)
            p_ids = data.get("pending_ids", [])
            idx = data.get("current_index", 0)
            if req_id in p_ids:
                p_ids.remove(req_id)
                await state.update_data(pending_ids=p_ids)
            if idx >= len(p_ids):
                idx = len(p_ids) - 1
            await state.update_data(current_index=idx)

        await state.clear()

    # ---- Принять правила (для "self") ----
    @dp.callback_query(F.data.startswith("accept_rules_"))
    async def accept_rules_bot(callback: CallbackQuery):
        req_id = int(callback.data.split("_")[2])
        
        # Проверяем, рабочее ли сейчас время
        if not is_work_time():
            # Сохраняем дату принятия правил
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with get_db() as db:
                req = db.get(UserRequest, req_id)
                if req:
                    req.rules_accepted_at = current_time
                    
                    # Создаем запись для отложенной отправки
                    new_pending = PendingInvite(
                        request_id=req_id,
                        chat_id=callback.from_user.id,
                        created_at=current_time,
                        is_third_party=0
                    )
                    db.add(new_pending)
                    db.commit()
            
            await callback.message.edit_text(
                "Вы приняли правила!\n\n"
                "⚠️ Ссылка на беседу будет отправлена автоматически в рабочее время (с 8:00 до 20:00 в будние дни)."
            )
            await callback.answer()
            return
        
        # Остальной код остается без изменений
        try:
            # Для обычных пользователей (self):
            link = await callback.message.bot.create_chat_invite_link(
                PRIVATE_GROUP_ID,
                member_limit=1  # Ограничение на 1 использование
            )
            
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Сохраняем дату принятия правил и уведомляем админов
            with get_db() as db:
                req = db.get(UserRequest, req_id)
                if req:
                    req.rules_accepted_at = current_time
                    db.commit()
                    
                    # Уведомляем админов
                    admins = db.query(AdminUser).all()
                    for admin in admins:
                        try:
                            await callback.message.bot.send_message(
                                chat_id=admin.telegram_id,
                                text=f"✅ Пользователь {req.full_name} (заявка #{req_id}) принял правила и получил ссылку на группу.\n📅 Дата: {current_time}"
                            )
                        except Exception as e:
                            logging.error(f"Ошибка отправки уведомления админу {admin.telegram_id}: {e}")

            await callback.message.edit_text(
                text=(
                    "Вы приняли правила!\n\n"
                    f"Вот ваша ссылка для вступления в группу:\n{link.invite_link}"
                )
            )
            await callback.answer()
        except Exception as e:
            logging.error(f"Ошибка при создании ссылки: {e}")
            await callback.answer("Ошибка при создании ссылки.", show_alert=True)

    # ---- Отклонить правила (для "self") ----
    @dp.callback_query(F.data.startswith("decline_rules_"))
    async def decline_rules_bot(callback: CallbackQuery):
        req_id = int(callback.data.split("_")[2])
        with get_db() as db:
            req = db.get(UserRequest, req_id)
            if req and req.status == "approved":
                req.status = "rejected"
                req.rejection_reason = "Пользователь не принял правила"
                db.commit()
        await callback.message.edit_text("Вы отклонили правила. Доступ в группу не предоставлен.")
        await callback.answer()

    # ---- /accept /decline (для третьего лица) ----
    @dp.message(Command("accept"))
    async def accept_command_third(message: Message):
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer("Использование: /accept <код>")
            return
        code = parts[1]
        
        with get_db() as db:
            req = db.query(UserRequest).filter_by(confirmation_code=code).first()
            if not req:
                await message.answer("Некорректный код. Заявка не найдена.")
                return
            if req.status != "approved":
                await message.answer("Заявка не в статусе 'approved'.")
                return
        
        # Проверяем, рабочее ли сейчас время
        if not is_work_time():
            # Сохраняем дату принятия правил
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with get_db() as db:
                req = db.query(UserRequest).filter_by(confirmation_code=code).first()
                req.rules_accepted_at = current_time
                
                # Создаем запись для отложенной отправки
                new_pending = PendingInvite(
                    request_id=req.id,
                    chat_id=message.from_user.id,
                    created_at=current_time,
                    is_third_party=1,
                    confirmation_code=code
                )
                db.add(new_pending)
                db.commit()
            
            await message.answer(
                "Спасибо! Вы приняли правила.\n\n"
                "⚠️ Ссылка на беседу будет отправлена автоматически в рабочее время (с 8:00 до 20:00 в будние дни)."
            )
            return
        
        # Остальной код остается без изменений
        with get_db() as db:
            try:
                # Для третьих лиц:
                link = await message.bot.create_chat_invite_link(
                    PRIVATE_GROUP_ID, 
                    member_limit=1  # Ограничение на 1 использование
                )
                
                # Сохраняем дату принятия правил
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                req.rules_accepted_at = current_time
                db.commit()

                # Уведомляем админов
                admins = db.query(AdminUser).all()
                for admin in admins:
                    try:
                        await message.bot.send_message(
                            chat_id=admin.telegram_id,
                            text=f"✅ Пользователь {req.full_name} (заявка #{req.id}) принял правила и получил ссылку на группу.\n📅 Дата: {current_time}"
                        )
                    except Exception as e:
                        logging.error(f"Ошибка отправки уведомления админу {admin.telegram_id}: {e}")

                await message.answer(
                    "Спасибо! Вы приняли правила.\n\n"
                    f"Ссылка для вступления в группу: {link.invite_link}"
                )
                
                # Очищаем код после использования
                req.confirmation_code = None
                db.commit()

            except Exception as e:
                logging.error(f"Ошибка при создании ссылки: {e}")
                await message.answer("Ошибка при создании ссылки.")

    @dp.message(Command("decline"))
    async def decline_command_third(message: Message):
        parts = message.text.split()
        if len(parts) < 2:
            await message.answer("Использование: /decline <код>")
            return
        code = parts[1]
        with get_db() as db:
            req = db.query(UserRequest).filter_by(confirmation_code=code).first()
            if not req:
                await message.answer("Некорректный код. Заявка не найдена.")
                return
            if req.status != "approved":
                await message.answer("Заявка не в статусе 'approved'.")
                return

            req.status = "rejected"
            req.rejection_reason = "Третье лицо не приняло правила"
            req.confirmation_code = None
            db.commit()

        await message.answer("Вы отклонили правила. Доступ в группу не предоставлен.")

    # ---- Обработчик входа пользователя в группу ----
    @dp.message(F.new_chat_members)
    async def on_user_join(message: Message):
        for new_member in message.new_chat_members:
            with get_db() as db:
                # Ищем самую последнюю заявку от этого пользователя
                req = db.query(UserRequest).filter_by(
                    chat_id=new_member.id
                ).order_by(
                    UserRequest.id.desc()  # сортируем по id в обратном порядке
                ).first()  # берем первую (самую новую)
                
                if req:
                    # Проверяем, рабочее ли сейчас время
                    if not is_work_time():
                        # Сохраняем информацию для отложенной отправки
                        new_notification = PendingJoinNotification(
                            user_id=new_member.id,
                            chat_id=message.chat.id,
                            full_name=req.full_name,
                            workplace=req.workplace,
                            position=req.position,
                            created_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        )
                        db.add(new_notification)
                        db.commit()
                    else:
                        # Если рабочее время, отправляем сразу
                        await message.answer(
                            f"👋 Добро пожаловать, {req.full_name}!\n"
                            f"🏢 Место работы: {req.workplace}\n"
                            f"💼 Должность: {req.position}\n"
                        )

    # ---- Контроль времени сообщений в группе ----
    @dp.message(F.chat.type.in_({"group", "supergroup"}))
    async def check_message_time(message: Message):
        now = datetime.now()       
        is_weekday = now.weekday() < 5
        is_work_hours = 8 <= now.hour < 20
        if not is_weekday or not is_work_hours:
            await message.reply(
                "Согласно правилам группы - писать только с 8 до 20 часов в будние дни"
            )



    # ---- Запуск бота ----
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())


