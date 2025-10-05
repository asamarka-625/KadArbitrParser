# Внешние зависимости
import json
from typing import Dict, List, Set
import time
import random
import requests
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
# Внутренние модули
from app.settings.config import get_config
from app.utils.gender_detector import RussianGenderDetector
from app.parsers.get_cookies import init_session_with_cookies


config = get_config()


class Parser:
    URL_POST = "https://kad.arbitr.ru/Kad/SearchInstances"
    URL_GET = "https://kad.arbitr.ru/"

    def __init__(self, date_from: str, date_to: str):
        self.ua = UserAgent()
        self.HEADERS = {
            "Host": "kad.arbitr.ru",
            "User-Agent": self.ua.random
        }
        self.PAYLOAD = {
            "Page": 1,
            "Count": 25,
            "Courts": ["SPB"],
            "DateFrom": f"{date_from}T00:00:00", # "2025-06-01T00:00:00"
            "DateTo": f"{date_to}T23:59:59", # "2025-08-06T23:59:59"
            "Sides": [],
            "Judges": [],
            "CaseNumbers": [],
            "WithVKSInstances": False,
            "CaseType": "B"
        }
        self.session = init_session_with_cookies(
            url="https://kad.arbitr.ru/",
            wait_for_cookies=['pr_fp', 'rcid', 'wasm']
        )
        # self.session = requests.Session()
        # self.set_cookies_from_file(filename=config.COOKIES_FOR_PARSER_PATH)

    def get_data(self) -> str:
        """Делаем POST запрос и получаем ответ"""
        config.logger.info("Делаем POST запрос на получение данных")

        try:
            response = self.session.post(
                url=self.URL_POST,
                data=self.PAYLOAD,
                headers=self.HEADERS
            )

            response.raise_for_status()

        except requests.HTTPError as err:
            config.logger.error(f"Ошибка! Не удалось сделать POST запрос. HTTPError: {err}")
            raise

        except Exception as err:
            config.logger.error(f"Ошибка! Не удалось сделать POST запрос. Error: {err}")
            raise

        else:
            config.logger.info("Ответ успешно получен!")
            return response.text

    def set_cookies_from_file(self, filename: str) -> None:
        """Устанавливаем сессионные куки из файла"""
        config.logger.info("Устанавливаем сессионные куки из файла")

        with open(file=f"app/{filename}", mode='r') as f:
            cookies_dict = json.load(f)
            self.session.cookies.update(cookies_dict)

    @staticmethod
    def data_processing(text: str, existing_ids_case: Set[str]) -> List[Dict[str, str]]:
        """Обрабатывает данные"""
        config.logger.info("Обрабатываем данные")

        detector = RussianGenderDetector()
        answer = []

        soup = BeautifulSoup(text,"html.parser")
        rows = soup.find_all('tr')

        for row in rows:
            td_num = row.find('td', class_='num')
            bankruptcy = td_num.find('div', class_='bankruptcy')

            if bankruptcy is None:
                continue

            date = bankruptcy.find('span').text.strip()
            case = td_num.find('a', class_='num_case', href=True)
            num_case = case.text.strip()

            if num_case in existing_ids_case:
                continue

            case_link = case['href']

            td_respondent = row.find('td', class_='respondent').find('span', class_='js-rollover b-newRollover')
            respondent_name = td_respondent.find('strong').text.strip()
            respondent_data = td_respondent.find('br').next_sibling.strip()

            if len([char for char in respondent_name if char.isupper()]) != 3:
                if respondent_name[:2] != "ИП":
                    continue

            el_respondent_name = respondent_name.split(" ")
            if len(el_respondent_name) == 3:
                fio = respondent_name

            elif len(el_respondent_name) == 4:
                fio = " ".join(el_respondent_name[1:])

            else:
                continue

            if not detector.detect_gender_with_fallback(fio=fio):
                continue

            if respondent_data != "Данные скрыты":
                if "Санкт-Петербург".lower() not in respondent_data.lower():
                    continue

            respondent_inn = td_respondent.find('div')
            respondent_inn = respondent_inn.text.strip().replace('ИНН: ', '') \
                if respondent_inn is not None else ''

            answer.append({
                "case": {
                    "date": date,
                    "num_case": num_case,
                    "case_link": case_link
                },
                "respondent": {
                    "name": respondent_name,
                    "data": respondent_data,
                    "inn": respondent_inn
                }
            })
            print(answer[-1])

        return answer

    def run_parse(self, existing_ids_case: Set[str]) -> List:
        config.logger.info("Запускаем парсер")
        num_page = 1
        result = []
        retry_limit = 3
        retry = 0

        while True:
            self.PAYLOAD["Page"] = num_page

            try:
                text = self.get_data()
                ans = self.data_processing(text=text, existing_ids_case=existing_ids_case)

                if len(ans) == 0:
                    if retry > retry_limit:
                        config.logger.info("Новых данных нет, заканчиваем парсинг...")
                        break
                    retry += 1
                    config.logger.info(f"Попытка получить данные retry: {retry}")

                for el in ans:
                    existing_ids_case.add(el["case"]["num_case"])

            except Exception as err:
                config.logger.error(f"Произошла ошибка, заканчиваем парсинг! Error: {err}")
                break

            else:
                result.extend(ans)
                num_page += 1
                time.sleep(random.randint(3, 6))

        config.logger.info(f"Получено новых дынных: {len(result)}")
        return result