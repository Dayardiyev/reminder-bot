import asyncio
import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, time
from pathlib import Path
from typing import Dict, List, Optional

from telegram import ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

STORAGE_PATH = Path("data/events.json")
DAILY_REMINDER_TIME = time(hour=9, minute=0)
REMINDER_OFFSETS = [
    timedelta(hours=10),
    timedelta(hours=5),
    timedelta(minutes=30),
    timedelta(minutes=5),
    timedelta(seconds=0),
]
MENU_KEYBOARD = ReplyKeyboardMarkup(
    [["Добавить событие", "Список событий"]],
    resize_keyboard=True,
)

CANCEL_KEYBOARD = ReplyKeyboardMarkup(
    [["Отмена"]],
    resize_keyboard=True,
    one_time_keyboard=True,
)

TITLE, DATE = range(2)


@dataclass
class Event:
    id: str
    title: str
    when: datetime
    chat_id: int

    @classmethod
    def from_dict(cls, raw: Dict) -> "Event":
        return cls(
            id=raw["id"],
            title=raw["title"],
            when=datetime.fromisoformat(raw["when"]),
            chat_id=raw["chat_id"],
        )

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "title": self.title,
            "when": self.when.isoformat(),
            "chat_id": self.chat_id,
        }


class EventStore:
    def __init__(self, path: Path):
        self.path = path
        self._lock = asyncio.Lock()

    async def _read(self) -> Dict[str, List[Event]]:
        if not self.path.exists():
            return {}
        with open(self.path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return {user_id: [Event.from_dict(item) for item in items] for user_id, items in raw.items()}

    async def _write(self, data: Dict[str, List[Event]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        raw = {user_id: [event.to_dict() for event in items] for user_id, items in data.items()}
        tmp_path = self.path.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(raw, f, ensure_ascii=False, indent=2)
        tmp_path.replace(self.path)

    async def load_all(self) -> Dict[str, List[Event]]:
        async with self._lock:
            return await self._read()

    async def list_events(self, user_id: int) -> List[Event]:
        async with self._lock:
            data = await self._read()
            return sorted(data.get(str(user_id), []), key=lambda item: item.when)

    async def add_event(self, user_id: int, event: Event) -> None:
        async with self._lock:
            data = await self._read()
            user_key = str(user_id)
            events = data.get(user_key, [])
            events.append(event)
            events.sort(key=lambda item: item.when)
            data[user_key] = events
            await self._write(data)


def parse_datetime(raw: str) -> Optional[datetime]:
    cleaned = raw.strip()
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(cleaned, fmt)
            if fmt == "%Y-%m-%d":
                # Default time for date-only input.
                parsed = datetime.combine(parsed.date(), time(hour=9, minute=0))
            return parsed
        except ValueError:
            continue
    return None


def humanize_timedelta(delta: timedelta) -> str:
    total_minutes = int(delta.total_seconds() // 60)
    hours, minutes = divmod(total_minutes, 60)
    parts = []
    if hours:
        parts.append(f"{hours} ч")
    if minutes or not parts:
        parts.append(f"{minutes} мин")
    return " ".join(parts)


def remove_existing_jobs(application: Application, name: str) -> None:
    for job in application.job_queue.get_jobs_by_name(name):
        job.schedule_removal()


def schedule_event_jobs(application: Application, user_id: int, event: Event) -> None:
    now = datetime.now()
    if event.when <= now:
        return
    schedule_daily_counter(application, user_id, event, now)
    schedule_event_reminders(application, user_id, event, now)


async def create_event_and_schedule(
        user_id: int,
        chat_id: int,
        title: str,
        when: datetime,
        store: EventStore,
        application: Application,
) -> Event:
    event = Event(
        id=str(uuid.uuid4()),
        title=title,
        when=when,
        chat_id=chat_id,
    )
    await store.add_event(user_id, event)
    schedule_event_jobs(application, user_id, event)
    return event


def schedule_daily_counter(application: Application, user_id: int, event: Event, now: datetime) -> None:
    job_name = f"daily-{user_id}-{event.id}"
    remove_existing_jobs(application, job_name)

    today_target = datetime.combine(now.date(), DAILY_REMINDER_TIME)
    first_run = today_target if today_target > now else today_target + timedelta(days=1)
    delay = (first_run - now).total_seconds()

    application.job_queue.run_repeating(
        daily_countdown,
        interval=timedelta(days=1),
        first=delay,
        name=job_name,
        data={"user_id": user_id, "event": event},
    )


def schedule_event_reminders(application: Application, user_id: int, event: Event, now: datetime) -> None:
    for offset in REMINDER_OFFSETS:
        when = event.when - offset
        if when <= now:
            continue
        job_name = f"reminder-{user_id}-{event.id}-{int(offset.total_seconds())}"
        remove_existing_jobs(application, job_name)
        delay = (when - now).total_seconds()
        application.job_queue.run_once(
            event_reminder,
            when=delay,
            name=job_name,
            data={"user_id": user_id, "event": event, "offset": offset},
        )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user and update.effective_chat:
        print(
            f"/start from user_id={update.effective_user.id} "
            f"username={update.effective_user.username} "
            f"chat_id={update.effective_chat.id}",
            flush=True,
        )
    await update.message.reply_text(
        "Привет! Я помогу следить за событиями.\n"
        "Нажми «Добавить событие» или «Список событий» ниже.\n"
        "Можно также использовать команды: /add и /list",
        reply_markup=MENU_KEYBOARD,
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Кнопки: «Добавить событие» и «Список событий» под строкой ввода.\n"
        "Команды: /add Название; YYYY-MM-DD HH:MM чтобы добавить событие.\n"
        "Можно указать только дату, тогда время по умолчанию 09:00.\n"
        "Команда /list покажет все твои события.\n"
        "Отмена ввода — /cancel или кнопка Отмена."
    )


async def add_event_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user or not update.effective_chat:
        return

    if not context.args:
        await update.message.reply_text("Формат: /add Название; 2024-12-31 18:00", reply_markup=MENU_KEYBOARD)
        return

    payload = update.message.text.split(" ", 1)
    if len(payload) < 2 or ";" not in payload[1]:
        await update.message.reply_text("Нужно использовать точку с запятой: /add Название; 2024-12-31 18:00", reply_markup=MENU_KEYBOARD)
        return

    title_raw, date_raw = [part.strip() for part in payload[1].split(";", 1)]
    if not title_raw:
        await update.message.reply_text("Название не может быть пустым.", reply_markup=MENU_KEYBOARD)
        return

    when = parse_datetime(date_raw)
    if not when:
        await update.message.reply_text("Не получилось разобрать дату. Формат: YYYY-MM-DD HH:MM (например, 2024-12-31 18:00).", reply_markup=MENU_KEYBOARD)
        return

    now = datetime.now()
    if when <= now:
        await update.message.reply_text("Дата должна быть в будущем.", reply_markup=MENU_KEYBOARD)
        return

    store: EventStore = context.application.bot_data["store"]
    event = await create_event_and_schedule(
        user_id=update.effective_user.id,
        chat_id=update.effective_chat.id,
        title=title_raw,
        when=when,
        store=store,
        application=context.application,
    )

    await update.message.reply_text(
        f"Событие \"{event.title}\" добавлено на {event.when.strftime('%Y-%m-%d %H:%M')}.",
        reply_markup=MENU_KEYBOARD,
    )


async def list_events_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return

    await send_events_list(update, context)


async def send_events_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: EventStore = context.application.bot_data["store"]
    events = await store.list_events(update.effective_user.id)
    if not events:
        await update.message.reply_text("Событий пока нет. Добавь через /add", reply_markup=MENU_KEYBOARD)
        return

    now = datetime.now()
    lines = []
    for event in events:
        status: str
        if event.when < now:
            status = "уже прошло"
        else:
            time_left = event.when - now
            hours_left = time_left.total_seconds() / 3600

            if hours_left < 24:
                hours = int(hours_left)
                status = f"через {hours} ч."
            else:
                days_left = (event.when.date() - now.date()).days
                if days_left == 1:
                    status = "завтра"
                else:
                    status = f"через {days_left} дней"
        lines.append(f"- {event.title} — {event.when.strftime('%Y-%m-%d %H:%M')} ({status})")

    await update.message.reply_text("Твои события:\n" + "\n".join(lines), reply_markup=MENU_KEYBOARD)


async def list_events_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.effective_user:
        return
    await send_events_list(update, context)


async def add_event_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.effective_user:
        return ConversationHandler.END
    await update.message.reply_text(
        "Как назовем событие?",
        reply_markup=CANCEL_KEYBOARD,
    )
    return TITLE


async def add_event_title(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.effective_user:
        return ConversationHandler.END
    title = update.message.text.strip()
    if not title or title.lower() == "отмена":
        await update.message.reply_text("Добавление отменено.", reply_markup=MENU_KEYBOARD)
        return ConversationHandler.END
    context.user_data["new_event_title"] = title
    await update.message.reply_text(
        "Укажи дату и время в формате YYYY-MM-DD HH:MM (можно только дату).",
        reply_markup=CANCEL_KEYBOARD,
    )
    return DATE


async def add_event_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message or not update.effective_user or not update.effective_chat:
        return ConversationHandler.END
    text = update.message.text.strip()
    if text.lower() == "отмена":
        await update.message.reply_text("Добавление отменено.", reply_markup=MENU_KEYBOARD)
        return ConversationHandler.END

    when = parse_datetime(text)
    if not when:
        await update.message.reply_text(
            "Не получилось разобрать дату. Формат: YYYY-MM-DD HH:MM (например, 2024-12-31 18:00).\n"
            "Попробуй ещё раз или нажми Отмена.",
            reply_markup=CANCEL_KEYBOARD,
        )
        return DATE

    if when <= datetime.now():
        await update.message.reply_text(
            "Дата должна быть в будущем. Попробуй ещё раз или Отмена.",
            reply_markup=CANCEL_KEYBOARD,
        )
        return DATE

    title = context.user_data.get("new_event_title", "Без названия")
    store: EventStore = context.application.bot_data["store"]
    event = await create_event_and_schedule(
        user_id=update.effective_user.id,
        chat_id=update.effective_chat.id,
        title=title,
        when=when,
        store=store,
        application=context.application,
    )
    context.user_data.pop("new_event_title", None)
    await update.message.reply_text(
        f"Событие \"{event.title}\" добавлено на {event.when.strftime('%Y-%m-%d %H:%M')}.",
        reply_markup=MENU_KEYBOARD,
    )
    return ConversationHandler.END


async def cancel_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message:
        await update.message.reply_text("Добавление отменено.", reply_markup=MENU_KEYBOARD)
    context.user_data.pop("new_event_title", None)
    return ConversationHandler.END


async def daily_countdown(context: ContextTypes.DEFAULT_TYPE) -> None:
    job_data = context.job.data
    event: Event = job_data["event"]
    user_id = job_data["user_id"]

    now = datetime.now()
    if event.when <= now:
        context.job.schedule_removal()
        return

    days_left = (event.when.date() - now.date()).days
    await context.bot.send_message(
        chat_id=event.chat_id,
        text=f"До события \"{event.title}\" осталось {days_left} дней.",
    )


async def event_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    job_data = context.job.data
    event: Event = job_data["event"]
    offset: timedelta = job_data["offset"]

    if offset.total_seconds() <= 0:
        text = f"Событие \"{event.title}\" начинается сейчас!"
    else:
        text = f"Событие \"{event.title}\" через {humanize_timedelta(offset)}."

    await context.bot.send_message(chat_id=event.chat_id, text=text)


async def reschedule_all_events(application: Application, store: EventStore) -> None:
    data = await store.load_all()
    now = datetime.now()
    for user_id_str, events in data.items():
        try:
            user_id = int(user_id_str)
        except ValueError:
            continue
        for event in events:
            if event.when > now:
                schedule_event_jobs(application, user_id, event)


async def _post_init(application: Application) -> None:
    store: EventStore = application.bot_data["store"]
    await reschedule_all_events(application, store)


def main() -> None:
    logging.basicConfig(
        format="%(asctime)s %(name)s [%(levelname)s]: %(message)s",
        level=logging.INFO,
    )
    token = os.environ.get("BOT_TOKEN")
    if not token:
        raise RuntimeError("Environment variable BOT_TOKEN is required.")

    request = HTTPXRequest(
        http_version="1.1",
        connect_timeout=20.0,
        read_timeout=20.0,
    )

    store = EventStore(STORAGE_PATH)
    application = (
        Application.builder()
        .token(token)
        .request(request)
        .post_init(_post_init)
        .build()
    )
    application.bot_data["store"] = store

    if application.job_queue is None:
        raise RuntimeError(
            "JobQueue недоступен. Установи зависимости с job-queue: "
            "pip install -r requirements.txt (нужен extras python-telegram-bot[job-queue])."
        )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("add", add_event_command))
    application.add_handler(CommandHandler("list", list_events_command))
    application.add_handler(
        ConversationHandler(
            entry_points=[
                MessageHandler(filters.Regex("^Добавить событие$"), add_event_start),
            ],
            states={
                TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_event_title)],
                DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_event_date)],
            },
            fallbacks=[
                CommandHandler("cancel", cancel_add),
                MessageHandler(filters.Regex("^Отмена$"), cancel_add),
            ],
            allow_reentry=True,
        )
    )
    application.add_handler(MessageHandler(filters.Regex("^Список событий$"), list_events_button))

    application.run_polling()


if __name__ == "__main__":
    main()