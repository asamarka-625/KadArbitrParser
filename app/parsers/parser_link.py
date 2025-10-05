# Внешние зависимости
from typing import Optional, Dict
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
# Внутренние модули
from app.parsers.get_cookies import SeleniumCookieManager
from app.settings.config import get_config


config = get_config()


class ParserLinks(SeleniumCookieManager):
    def __init__(self, headless: bool = True):
        super().__init__(headless)

    def get_pdf_link_after_click(self, timeout: int = 10) -> Optional[str]:
        """Получить только PDF ссылку из появившегося элемента"""
        config.logger.info(f"Ищем ссылку на PDF файл")

        try:
            # Ждем появление элемента
            container = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.CLASS_NAME, "b-case-chrono-ed"))
            )

            all_links = container.find_elements(By.TAG_NAME, "a")

            for link in all_links:
                href = link.get_attribute("href")
                if href and ".pdf" in href.lower():
                    config.logger.info(f"Найдена PDF ссылка: {href}")
                    return href

            config.logger.info(f"Ссылка на PDF файл не найдена")

        except Exception as e:
            config.logger.error(f"Ошибка при получении PDF ссылок: {e}")

    def run(self, url_card):
        self.get_cookies_with_selenium(
            url=url_card,
            wait_for_cookies=['pr_fp', 'rcid', 'wasm'],
            click_object="b-case-chrono-button",
            button_index=2
        )

        link_pdf = self.get_pdf_link_after_click()

        return link_pdf


def parser_link_PDF_from_cards(cards: Dict[str, str]) -> Dict[str, Optional[str]]:
    result = {}
    parser = ParserLinks()
    parser.setup_driver()
    
    i = 0
    data = [(key, value) for key, value in cards.items()]
    
    while i < len(cards):
        id_card, url_card = data
        try:
            if i % 10 == 0 and i != 0:
                parser.close()
                
                parser = ParserLinks()
                parser.setup_driver()

            config.logger.info(f"[{i+1}/{len(cards)}] Поиск ссылки на PDF файл дела {id_card}")
            link_pdf = parser.run(url_card)
        
        except:
            time.sleep(300)
            
        else:
            result[id_card] = link_pdf
            i += 1
    
    parser.close()
    
    return result


if __name__ == "__main__":
    cards = {
        "А56-93992/2025": "https://kad.arbitr.ru/Card/0e393647-586d-4a59-8a99-85eac1890211",
        "А56-94250/2025": "https://kad.arbitr.ru/Card/02c4b7c5-9614-4fed-b4ee-921f761c7218"
    }
    result = parser_link_PDF_from_cards(cards)
    print(result)