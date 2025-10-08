# Внешние зависимости
import json
from typing import Tuple, Set, List
from gspread import Client, Spreadsheet, service_account
# Внутренние модули
from app.settings.config import get_config


config = get_config()


class GoogleTable:
    def __init__(self):
        self.client = self.client_init_json(filename=config.GOOGLE_KEYS_PATH)
        self.table = self.get_table_by_url(table_url=config.TABLE_URL)

    @staticmethod
    def client_init_json(filename: str) -> Client:
        """Создание клиента для работы с Google Sheets"""
        config.logger.info("Создаем клиента для работы с Google Sheets")
        return service_account(filename=f"{filename}")

    def get_table_by_url(self, table_url: str) -> Spreadsheet:
        """Получение таблицы из Google Sheets по ссылке"""
        config.logger.info("Получаем таблицы из Google Sheets по ссылке")
        return self.client.open_by_url(table_url)

    def get_worksheet_info(self) -> dict:
        """Возвращает количество листов в таблице и их названия"""
        config.logger.info("Возвращает количество листов в таблице и их названия")

        worksheets = self.table.worksheets()
        worksheet_info = {
            "count": len(worksheets),
            "names": [worksheet.title for worksheet in worksheets]
        }
        return worksheet_info

    def insert_one(self, title: str, data: list, index: int = 1):
        """Вставка данных в лист"""
        config.logger.info("Вставляем данные в строку")

        worksheet = self.table.worksheet(title)
        worksheet.insert_row(data, index=index)

    def insert_data(self, data: list, start_row: int = 2, worksheet_num: int = 0):
        config.logger.info("Вставляем данные в лист")

        # Определяем фиксированные заголовки
        headers = [
            'Дата',
            'Дело',
            'Ссылка',
            'ФИО Ответчика',
            'Адрес проживания',
            'ИНН'
        ]

        config.logger.info("Преобразуем данные -> вложенный словарь в плоский")

        flat_data = []
        for item in data:
            flat_item = self.flatten_structure(item)
            flat_data.append(flat_item)

        # Создаем строки для вставки
        rows = []
        for row in flat_data:
            row_data = [
                row.get('case_date', ''),  # Дата
                row.get('case_num_case', ''),  # Дело
                row.get('case_case_link', ''),  # Ссылка
                row.get('respondent_name', ''),  # ФИО Ответчик
                row.get('respondent_inn', ''),  # ИНН
                row.get('respondent_data', ''),  # Адрес проживания
                row.get('respondent_district', ''),  # Район
            ]
            rows.append(row_data)

        worksheet_info = self.get_worksheet_info()
        worksheet = self.table.worksheet(worksheet_info['names'][worksheet_num])

        # Вставляем заголовки если это первая вставка
        if start_row == 1:
            worksheet.insert_row(headers, index=start_row)
            start_row += 1

        try:
            worksheet.insert_rows(rows, row=start_row)

        except Exception as err:
            config.logger.error(f"Ошибка! Не удалось вставить. Error: {err}")

        else:
            config.logger.info(f"Данные успешно вставлены! Кол-во вставленных данных: {len(rows)}")

    def flatten_structure(self, data: dict, parent_key='', sep='_'):
        """Преобразует вложенный словарь в плоский"""
        items = []
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k

            if isinstance(v, dict):
                items.extend(self.flatten_structure(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))

        return dict(items)

    @staticmethod
    def get_data(filename: str) -> list:
        """Получаем данные из файла"""
        config.logger.info("Получаем данные из json файла")

        data = []
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)

        result = []
        for objects in data:
            result.extend(objects['data'])

        return result

    def get_all_ids_case(self) -> Tuple[int, Set[str]]:
        """Идентификаторы всех дел из таблицы"""
        config.logger.info("Получаем идентификаторы всех дел из таблицы")

        worksheet_info = self.get_worksheet_info()
        worksheet = self.table.worksheet(worksheet_info['names'][config.WORKSHEET_NUM])

        all_rows = worksheet.get_all_values()

        ids_case = set()
        for row in all_rows:
            ids_case.add(row[1])

        return len(ids_case), ids_case

    def clear_all_rows_except_first(self, start_del_row: int = 2, worksheet_num: int = 0):
        """Удаляет все строки в таблице, кроме первой (заголовков)"""
        try:
            worksheet_info = self.get_worksheet_info()
            worksheet = self.table.worksheet(worksheet_info['names'][worksheet_num])

            # Получаем количество строк в таблице
            row_count = worksheet.row_count

            # Если в таблице больше 1 строки, удаляем все начиная со второй
            if row_count > 1:
                config.logger.info(f"Удаляем {row_count - (start_del_row - 1)} строк(и) из таблицы, оставляя только заголовки")

                # Удаляем строки со 2-й до последней
                worksheet.delete_rows(start_del_row, row_count)

                config.logger.info("Все строки кроме первой успешно удалены")
            else:
                config.logger.info("В таблице только одна строка или она пуста, удаление не требуется")

        except Exception as e:
            config.logger.error(f"Ошибка при удалении строк: {e}")
            raise

    def get_all_data(self) -> Tuple[int, Set[str]]:
        """Вся информация из талицы из таблицы"""
        config.logger.info("Получаем всю информацию из таблицы")

        worksheet_info = self.get_worksheet_info()
        worksheet = self.table.worksheet(worksheet_info['names'][config.WORKSHEET_NUM])

        all_rows = worksheet.get_all_values()

        return all_rows

    def run_update_table(self, data: List, start_row: int):
        """Запускаем запись данных"""
        config.logger.info("Запускаем запись данных в таблицу")
        self.clear_all_rows_except_first(start_del_row=start_row, worksheet_num=config.WORKSHEET_NUM)
        self.insert_data(data=data, start_row=start_row, worksheet_num=config.WORKSHEET_NUM)