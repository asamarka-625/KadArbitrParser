# Внешние зависимости
import io
import re
from typing import Optional, Dict
import time
import PyPDF2
import requests
from fake_useragent import UserAgent
# Внутренние модули
from app.parsers.get_cookies import init_session_with_cookies
from app.settings.config import get_config


config = get_config()


class ParserPDF:
    def __init__(self):
        self.ua = UserAgent()
        self.HEADERS = {
            "Host": "kad.arbitr.ru",
            "User-Agent": self.ua.random
        }

        self.session = init_session_with_cookies(
            url="https://kad.arbitr.ru/",
            wait_for_cookies=['pr_fp', 'rcid', 'wasm']
        )

    def read_pdf_by_url(self, url: str) -> Optional[bytes]:
        """Получаем PDF файл"""
        try:
            kwargs_for_requests = {
                headers: self.HEADERS,
                data: {"RecaptchaToken": "undefined"},
                timeout: 15
            }
            
            if config.PROXY is not None:
                kwargs_for_requests["proxies"] = config.PROXIES
                
            response = self.session.post(url, **kwargs_for_requests)
            response.raise_for_status()

            return response.content

        except requests.HTTPError as err:
            config.logger.error(f"Ошибка запроса к PDF файлу. HTTPError: {err}")
            raise

        except Exception as err:
            config.logger.error(f"Ошибка запроса к PDF файлу. Error: {err}")
            raise

    def _parse_pdf_content(self, pdf_content: bytes) -> Optional[str]:
        """Парсим содержимое PDF"""
        config.logger.info("Парсим PDF контент")

        try:
            pdf_file = io.BytesIO(pdf_content)
            pdf_reader = PyPDF2.PdfReader(pdf_file)

            # Извлекаем текст
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text() + "\n"

            config.logger.info(f"PDF прочитан!")
            return text

        except Exception as err:
            config.logger.error(f"Ошибка парсинга PDF: {err}")
            return None

    @staticmethod
    def find_saint_petersburg_string(text: str) -> Optional[str]:
        """Находит строку начинающуюся с 'Санкт-Петербург' и заканчивающуюся на ';' или ')'"""
        config.logger.info("Ищем место жительства в файле")

        search = {}
        pattern_string_found = r'етербург[^;]*'

        try:
            # Паттерн для поиска строки от "Санкт" до ";" или ")"
            for trigger in ("жительства", "адрес", "регистр"):
                pattern = fr'(?:{trigger})[^;)]*[;)]'

                match = re.search(pattern, text)
                if match:
                    found_string = match.group()

                    # Если строка длиннее 90 символов, обрезаем
                    if len(found_string) > 90:
                        # Находим начало "Санкт-Петербург" и берем 90 символов
                        start_pos = match.start()
                        found_string = text[start_pos:start_pos + 90]
                        config.logger.info(f"Строка обрезана до 90 символов: {found_string}")

                    else:
                        config.logger.info(f"Найдена строка: {found_string}")

                    search[trigger] = found_string

            if len(search) == 0:
                for trigger in ("жительства", "адрес", "регистр"):
                    # Если не нашли с разделителем, ищем просто начало строки
                    pattern_start = fr'(?:{trigger})[^;)]*'
                    match_start = re.search(pattern_start, text)
                    if match_start:
                        found_string = match_start.group()
                        # Обрезаем до 90 символов если нужно
                        if len(found_string) > 90:
                            found_string = found_string[:90]
                            config.logger.info(f"Разделитель не найден, строка обрезана: {found_string}")

                        else:
                            config.logger.info(f"Разделитель не найден: {found_string}")

                        search[trigger] = found_string

            for trigger, string in search.items():
                match_found_string = re.search(pattern_string_found, string)
                if match_found_string:
                    return string.replace(trigger, '').replace(':', '').strip().replace('\n', '')

            return None

        except Exception as e:
            config.logger.error(f"Ошибка поиска строки: {e}")
            return None

    @staticmethod
    def find_inn_number(text: str) -> Optional[str]:
        """Находит последовательность цифр после слова 'ИНН' в любом регистре"""
        config.logger.info("Ищем ИНН в файле")

        try:
            # Паттерн: слово ИНН в любом регистре, потом пробелы, потом цифры до первого не-цифрового символа
            pattern = r'ИНН\s*(\d+)'

            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                inn_number = match.group(1)
                config.logger.info(f"Найден ИНН: {inn_number}")
                return inn_number

            else:
                config.logger.info("ИНН не найден")
                return None

        except Exception as e:
            config.logger.error(f"Ошибка поиска ИНН: {e}")
            return None

    def run_get_info_from_pfd(
            self,
            url: str,
            find_address: bool = True,
            find_inn: bool = True
    ) -> Dict[str, Optional[str]]:
        answer = {}

        content = self.read_pdf_by_url(url)
        text = self._parse_pdf_content(content)

        if text is not None:
            if find_address:
                address = self.find_saint_petersburg_string(text)
                answer["address"] = address

            if find_inn:
                inn = self.find_inn_number(text)
                answer["inn"] = inn
        
        else:
           raise ValueError("text is None") 

        return answer


def parser_PDF_file_from_links(cards: Dict) -> Dict[str, Dict[str, Optional[str]]]:
    result = {}
    i = 0
    card_ids = list(cards.keys())
    parser = ParserPDF()
    
    while i < len(card_ids):
        config.logger.info(f"[{i+1}/{len(cards)}] Поиск информации в PDF файле дела {id_card}")
        
        id_card = card_ids[i]
        data = cards[id_card]
        
        try:
            if i % 10 == 0 and i != 0:
                parser.close()
                parser = ParserPDF()
            
            info = parser.run_get_info_from_pfd(
                url=data["link_pdf"],
                find_address=data["find_address"],
                find_inn=data["find_inn"]
            )
            result[id_card] = info
        
        except ValueError:
            time.sleep(30)
            
        except:
            time.sleep(300)

        else:
            i += 1

    return result

