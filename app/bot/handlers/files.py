# Внешние зависимости
from aiogram import Router, types
from aiogram.filters import Command
from aiogram.types import FSInputFile
import os
import json


router = Router()


@router.message(Command("download_log"))
async def download_log(message: types.Message):
    """Скачивание файла log.log"""
    try:
        if not os.path.exists("log.log"):
            await message.answer("❌ Файл log.log не найден")
            return

        log_file = FSInputFile("log.log", filename="log.log")
        await message.answer_document(log_file, caption="📄 Файл логов")

    except Exception as e:
        await message.answer(f"❌ Ошибка при загрузке логов: {e}")


@router.message(Command("download_data"))
async def download_data(message: types.Message):
    """Скачивание файла data.json"""
    try:
        if not os.path.exists("data.json"):
            sample_data = {
                "users": [],
                "settings": {},
                "statistics": {
                    "total_messages": 0,
                    "active_users": 0
                }
            }
            with open("data.json", "w", encoding="utf-8") as f:
                json.dump(sample_data, f, ensure_ascii=False, indent=2)

        data_file = FSInputFile("data.json", filename="data.json")
        await message.answer_document(data_file, caption="📊 Файл данных")

    except Exception as e:
        await message.answer(f"❌ Ошибка при загрузке данных: {e}")


@router.message(Command("start"))
async def start_command(message: types.Message):
    """Команда старт"""
    welcome_text = """
🤖 Бот для управления задачами и уведомлениями

Доступные команды:
Команды:
/status - статус системы
/run_now - запустить задачу сейчас
/tasks - список задач
/stop_task NAME - остановить задачу
/stop_all_tasks - остановить все задачи
/download_log - скачать логи
/download_data - скачать данные
/gis_key_view - посмотреть ключ 2GIS
/gis_key_update - обновить ключ 2GIS
/gis_key_used_update - обновить кол-во использований ключа
"""
    await message.answer(welcome_text)