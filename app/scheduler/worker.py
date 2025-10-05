# Внешние зависимости
from datetime import datetime, timedelta
import time
import json
import asyncio
import threading
# Внутренние модули
from app.parsers.parser import Parser
from app.parsers.parser_link import parser_link_PDF_from_cards
from app.parsers.parser_pdf import parser_PDF_file_from_links
from app.table.google_table_work import GoogleTable
from app.settings.config import get_config
from app.bot.bot_manager import get_bot_manager


config = get_config()
bot_manager = get_bot_manager()


def get_data(range_days: int, delta_days: int, file_path: str):
    date_to = (datetime.now() - timedelta(days=delta_days)).strftime("%Y-%m-%d")
    date_from = (datetime.now() - timedelta(days=(range_days + delta_days))).strftime("%Y-%m-%d")

    data = []
    existing_ids_case = set()

    try:
        while True:
            parser = Parser(date_from=date_from, date_to=date_to)
            result = parser.run_parse(existing_ids_case)
            data.extend(result)

            if len(result) == 0:
                break

            date_last = data[-1]["case"]["date"]
            date_to = "-".join(date_last.split(".")[::-1])

            config.logger.info(f"date_to: {date_to}",)

            config.logger.info(f"len data: {len(data)}")

            config.logger.info(f"len existing_ids_case: {len(existing_ids_case)}")

    except KeyboardInterrupt:
        config.logger.info("(get_data) Получен сигнал остановки...")

    finally:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)


def get_links_PDF_from_data(file_path: str):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            config.logger.info(f"Файл {file_path} успешно прочитан")

    except FileNotFoundError:
        config.logger.error(f"(def get_links_PDF_from_data): Файл {file_path} не найден")
        return None

    except json.JSONDecodeError as e:
        config.logger.error(f"(def get_links_PDF_from_data): Ошибка декодирования JSON: {e}")
        return None

    except Exception as e:
        config.logger.error(f"(def get_links_PDF_from_data): Ошибка при чтении файла: {e}")
        return None

    else:
        cards = {}
        for el in data:
            respondent = el["respondent"]
            if respondent["data"] == "Данные скрыты" or respondent["inn"] == "":
                case = el["case"]
                case_id, case_link = case["num_case"], case["case_link"]
                cards[case_id] = case_link

        cards_link_PDF = parser_link_PDF_from_cards(cards)
        link_PDF_ids = cards_link_PDF.keys()

        new_data = []
        for el in data[::-1]:
            card_id = el["case"]["num_case"]
            if card_id in link_PDF_ids:
                if cards_link_PDF[card_id] is None:
                    continue

                el["case"]["pdf"] = cards_link_PDF[card_id]

            new_data.append(el)

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(new_data, f, ensure_ascii=False, indent=4)

        config.logger.info(f"Файл {file_path} успешно перезаписан")


def get_missing_info(file_path: str):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            config.logger.info(f"Файл {file_path} успешно прочитан")

    except FileNotFoundError:
        config.logger.error(f"(get_missing_info): Файл {file_path} не найден")
        return None

    except json.JSONDecodeError as e:
        config.logger.error(f"(get_missing_info): Ошибка декодирования JSON: {e}")
        return None

    except Exception as e:
        config.logger.error(f"(get_missing_info): Ошибка при чтении файла: {e}")
        return None

    else:
        cards = {}
        for el in data:
            if el["case"].get("pdf"):
                missing_info = {
                    "find_address": False,
                    "find_inn": False
                }
                case = el["case"]
                respondent = el["respondent"]

                if respondent["data"] == "Данные скрыты":
                    missing_info["find_address"] = True

                if respondent["inn"] == "":
                    missing_info["find_inn"] = True

                case_id = case["num_case"]
                cards[case_id] = {
                    "link_pdf": case["pdf"],
                    **missing_info
                }

        missing_info_cards = parser_PDF_file_from_links(cards)
        missing_info_ids = missing_info_cards.keys()

        new_data = []
        for el in data[::-1]:
            card_id = el["case"]["num_case"]
            if card_id in missing_info_ids:
                missing_address = missing_info_cards[card_id].get("address")
                missing_inn = missing_info_cards[card_id].get("inn")
                respondent = el["respondent"]

                if respondent["data"] == "Данные скрыты" and missing_address is None:
                    continue

                if respondent["data"] == "Данные скрыты" and missing_address is not None:
                    el["respondent"]["data"] = missing_address

                if respondent["inn"] == "" and missing_inn is not None:
                    el["respondent"]["inn"] = missing_inn

            new_data.append(el)

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(new_data, f, ensure_ascii=False, indent=4)

        config.logger.info(f"Файл {file_path} успешно перезаписан")


def update_table(file_path: str):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            config.logger.info(f"Файл {file_path} успешно прочитан")

    except FileNotFoundError:
        config.logger.error(f"(update_table): Файл {file_path} не найден")
        return None

    except json.JSONDecodeError as e:
        config.logger.error(f"(update_table): Ошибка декодирования JSON: {e}")
        return None

    except Exception as e:
        config.logger.error(f"(update_table): Ошибка при чтении файла: {e}")
        return None

    else:
        google_table = GoogleTable()
        google_table.run_update_table(data, 2)


def _send_step_notification(message: str, loop: asyncio.AbstractEventLoop):
    """Отправка уведомления о текущем шаге (без ожидания)"""
    try:
        asyncio.run_coroutine_threadsafe(bot_manager.send_notification(message), loop)
        # Просто логируем факт отправки, не ждем результат
        config.logger.debug(f"Уведомление поставлено в очередь: {message}")
        
    except Exception as e:
        config.logger.error(f"Ошибка планирования уведомления: {e}")
            
            
def main_task(loop: asyncio.AbstractEventLoop, range_days: int = 3, delta_days: int = 2, file_path: str = "data.json", stop_event=None):
    """Основная задача с уведомлениями о каждом шаге"""
    
    # Получаем ID потока для отладки
    thread_id = threading.current_thread().ident
    task_type = "Ручная" if "manual" in threading.current_thread().name else "Запланированная"
    
    try:
        _send_step_notification(f"🟡 {task_type} задача начата (Поток: {thread_id})", loop=loop)
        
        # Шаг 1: Получение данных
        _send_step_notification("🟡 Шаг 1: Получение данных...", loop=loop)
        get_data(range_days=range_days, delta_days=delta_days, file_path=file_path)
        _send_step_notification("✅ Шаг 1 завершен: Данные получены", loop=loop)

        # Шаг 2: Получение ссылок PDF
        _send_step_notification("🟡 Шаг 2: Получение ссылок на PDF...", loop=loop)
        get_links_PDF_from_data(file_path=file_path)
        _send_step_notification("✅ Шаг 2 завершен: Ссылки на PDF получены", loop=loop)

        # Шаг 3: Получение недостающей информации
        _send_step_notification("🟡 Шаг 3: Получение недостающей информации...", loop=loop)
        get_missing_info(file_path=file_path)
        _send_step_notification("✅ Шаг 3 завершен: Недостающая информация получена", loop=loop)
        
        # Шаг 4: Запись в таблицу
        _send_step_notification("🟡 Шаг 4: Запись данных в таблицу...", loop=loop)
        update_table(file_path=file_path)
        _send_step_notification("✅ Шаг 4 завершен: Данные записаны", loop=loop)

        _send_step_notification("🎉 Все задачи успешно выполнены!", loop=loop)
    
    except Exception as e:
        error_msg = f"❌ Критическая ошибка в {task_type.lower()} задаче (Поток: {thread_id}): {str(e)}"
        _send_step_notification(error_msg, loop=loop)
        config.logger.error(error_msg)