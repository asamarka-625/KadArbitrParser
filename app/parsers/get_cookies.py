# Внешние зависимости
import random
import time
from typing import Optional
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium_stealth import stealth
# Внутренние модули
from app.settings.config import get_config


config = get_config()


class SeleniumCookieManager:
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.driver = None
        self.session = requests.Session()

    def setting_options(self):
        config.logger.info("Настраиваем парараметры Chrome драйвера")

        chrome_options = Options()

        # Базовые настройки
        if self.headless:
            chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')

        # STEALTH НАСТРОЙКИ
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # Случайный user-agent
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        ]

        chrome_options.add_argument(f'--user-agent={random.choice(user_agents)}')

        config.logger.info("Параметры Chrome драйвера настроены!")

        return chrome_options

    def setup_driver(self):
        """Настройка Chrome драйвера с selenium-stealth"""
        config.logger.info("Создаем Chrome драйвер с stealth...")

        try:
            chrome_options = self.setting_options()

            self.driver = webdriver.Chrome(options=chrome_options)

            # ПРИМЕНЯЕМ STEALTH
            stealth(self.driver,
                    languages=["ru-RU", "ru", "en-US", "en"],
                    vendor="Google Inc.",
                    platform="Win32",
                    webgl_vendor="Intel Inc.",
                    renderer="Intel Iris OpenGL Engine",
                    fix_hairline=True,
                    run_on_insecure_origins=True,
                    )

            config.logger.info("Stealth Chrome драйвер инициализирован")

        except Exception as e:
            config.logger.error(f"Ошибка инициализации stealth драйвера: {e}")
            raise

    def get_cookies_with_selenium(
            self,
            url: str,
            wait_for_cookies: list = None,
            timeout: int = 30,
            click_object = "bankruptcy",
            button_index = None
    ):
        """
        Получение кук через Selenium с stealth настройками
        """
        if not self.driver:
            self.setup_driver()

        config.logger.info(f"Stealth загрузка страницы: {url}")

        try:
            # Stealth навигация
            self.driver.get(url)

            # Ждем загрузки страницы
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # Эмуляция человеческого поведения
            self._simulate_human_behavior()

            # Закрываем popup stealth способом
            self.stealth_close_popup()

            time.sleep(3)

            self.stealth_click_object(click_object, button_index=button_index)

            # Дополнительное время для выполнения JavaScript
            time.sleep(3)

            # Ждем выполнения JavaScript и появления нужных кук
            if wait_for_cookies:
                self._wait_for_specific_cookies(wait_for_cookies, timeout)

            # Получаем все куки
            cookies = self.driver.get_cookies()
            config.logger.info(f"Получено кук через Selenium: {len(cookies)}")

            # Переносим куки в requests сессию
            self._transfer_cookies_to_requests(selenium_cookies=cookies, url=url)

            return cookies

        except Exception as e:
            config.logger.error(f"Ошибка получения кук: {e}")
            raise

    def _simulate_human_behavior(self):
        """Эмуляция человеческого поведения"""
        try:
            # Случайный скролл
            scroll_amount = random.randint(100, 400)
            self.driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
            time.sleep(random.uniform(0.5, 1.5))

            # Случайные движения мыши
            actions = ActionChains(self.driver)
            actions.move_by_offset(
                random.randint(-100, 100),
                random.randint(-50, 50)
            )
            actions.pause(random.uniform(0.2, 0.8))
            actions.perform()

            config.logger.info("Человеческое поведение сэмулировано")

        except Exception as e:
            config.logger.debug(f"⚠️ Ошибка эмуляции поведения: {e}")

    def stealth_close_popup(self):
        """Stealth закрытие popup"""
        config.logger.info("Stealth закрытие popup...")

        try:
            # JavaScript для stealth закрытия popup
            self.driver.execute_script("""
                // Закрываем все возможные popup stealth способом
                function stealthClosePopups() {
                    // Закрываем через крестик
                    var closeButtons = [
                        'a.b-promo_notification-popup-close',
                        'a.js-promo_notification-popup-close', 
                        '.b-promo_notification-popup-close'
                    ];

                    closeButtons.forEach(function(selector) {
                        var btn = document.querySelector(selector);
                        if (btn) {
                            var event = new MouseEvent('click', {
                                view: window,
                                bubbles: true,
                                cancelable: true
                            });
                            btn.dispatchEvent(event);
                        }
                    });

                    // Удаляем popup контейнеры
                    var popupSelectors = [
                        'div.b-promo_notification-popup-cell',
                        '.b-promo_notification-popup'
                    ];

                    popupSelectors.forEach(function(selector) {
                        var popup = document.querySelector(selector);
                        if (popup) {
                            popup.style.display = 'none';
                            popup.style.visibility = 'hidden';
                            popup.remove();
                        }
                    });
                }

                stealthClosePopups();
            """)

            time.sleep(2)
            config.logger.info("Popup закрыт stealth способом")

        except Exception as e:
            config.logger.error(f"Ошибка stealth закрытия popup: {e}")

    def stealth_click_object(self, class_name: str, button_index: Optional[int] = None):
        """Stealth клик по class_name"""
        config.logger.info(f"Stealth клик по {class_name}...")

        try:
            if button_index is None:
                # Находим элемент
                element = WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, class_name))
                )

            else:
                # Находим все кнопки
                buttons = WebDriverWait(self.driver, 15).until(
                    EC.presence_of_all_elements_located((By.CLASS_NAME, "b-case-chrono-button"))
                )

                if button_index >= len(buttons):
                    config.logger.error(f"Кнопка с индексом {button_index} не найдена. Всего кнопок: {len(buttons)}")
                    return False

                element = buttons[button_index]

            # Эмуляция человеческого поведения перед кликом
            self._human_like_click_behavior(element)

            # Stealth клик
            success = self._perform_stealth_click(element)

            if success:
                config.logger.info("Stealth клик выполнен успешно!")
                return True
            else:
                config.logger.error("Stealth клик не сработал")
                return False

        except Exception as e:
            config.logger.error(f"Ошибка stealth клика: {e}")
            return False

    def _human_like_click_behavior(self, element):
        """Эмуляция человеческого поведения перед кликом"""
        try:
            actions = ActionChains(self.driver)

            # Двигаемся к элементу не прямо
            actions.move_by_offset(random.randint(-50, 50), random.randint(-30, 30))
            actions.pause(random.uniform(0.1, 0.3))
            actions.move_to_element(element)
            actions.pause(random.uniform(0.2, 0.5))
            actions.perform()

            # Случайный микро-скролл
            self.driver.execute_script(f"window.scrollBy(0, {random.randint(-10, 10)});")

        except Exception as e:
            config.logger.debug(f"Ошибка эмуляции поведения клика: {e}")

    def _perform_stealth_click(self, element):
        """Выполнение stealth клика"""
        click_methods = [
            self._stealth_js_click,
            self._stealth_mouse_event,
            self._stealth_dispatch_event,
        ]

        for method in click_methods:
            try:
                if method(element):
                    config.logger.info(f"Stealth клик выполнен методом: {method.__name__}")
                    return True
            except Exception as e:
                config.logger.debug(f"Метод {method.__name__} не сработал: {e}")
                continue

        return False

    def _stealth_js_click(self, element):
        """Stealth JavaScript клик"""
        self.driver.execute_script("""
            var element = arguments[0];

            // Очищаем следы автоматизации
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
            delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;

            // Выполняем клик
            element.click();
        """, element)
        time.sleep(1)
        return True

    def _stealth_mouse_event(self, element):
        """Stealth клик через MouseEvent"""
        self.driver.execute_script("""
            var element = arguments[0];
            var rect = element.getBoundingClientRect();

            // Случайные координаты внутри элемента
            var x = rect.left + rect.width / 2 + (Math.random() * 10 - 5);
            var y = rect.top + rect.height / 2 + (Math.random() * 10 - 5);

            // Полная последовательность событий мыши
            var mouseOver = new MouseEvent('mouseover', { bubbles: true });
            var mouseMove = new MouseEvent('mousemove', { 
                bubbles: true,
                clientX: x,
                clientY: y
            });
            var mouseDown = new MouseEvent('mousedown', { 
                bubbles: true,
                clientX: x,
                clientY: y,
                button: 0
            });
            var mouseUp = new MouseEvent('mouseup', { 
                bubbles: true,
                clientX: x,
                clientY: y,
                button: 0
            });
            var clickEvent = new MouseEvent('click', { 
                bubbles: true,
                clientX: x,
                clientY: y,
                button: 0
            });

            // Вызываем события с задержками
            element.dispatchEvent(mouseOver);
            setTimeout(function() {
                element.dispatchEvent(mouseMove);
                setTimeout(function() {
                    element.dispatchEvent(mouseDown);
                    setTimeout(function() {
                        element.dispatchEvent(mouseUp);
                        element.dispatchEvent(clickEvent);
                    }, 50 + Math.random() * 50);
                }, 30 + Math.random() * 40);
            }, 20 + Math.random() * 30);
        """, element)

        time.sleep(1)
        return True

    def _stealth_dispatch_event(self, element):
        """Stealth клик через dispatchEvent"""
        self.driver.execute_script("""
            var element = arguments[0];

            function stealthTriggerEvent(element, eventName) {
                var event = new Event(eventName, {
                    bubbles: true,
                    cancelable: true
                });
                element.dispatchEvent(event);
            }

            // Триггерим все связанные события
            stealthTriggerEvent(element, 'focus');
            stealthTriggerEvent(element, 'mouseenter');
            stealthTriggerEvent(element, 'mouseover');
            stealthTriggerEvent(element, 'mousedown'); 
            stealthTriggerEvent(element, 'mouseup');
            stealthTriggerEvent(element, 'click');

            // Вызываем обработчики если есть
            if (element.onclick) element.onclick();
            if (element.onmousedown) element.onmousedown();
        """, element)
        time.sleep(1)
        return True

    # Остальные методы остаются без изменений
    def _wait_for_specific_cookies(self, required_cookies: list, timeout: int):
        """Ожидание появления конкретных кук"""
        config.logger.info(f"Ожидаем появления кук: {required_cookies}")

        start_time = time.time()
        while time.time() - start_time < timeout:
            current_cookies = self.driver.get_cookies()
            current_cookie_names = {cookie['name'] for cookie in current_cookies}

            missing_cookies = set(required_cookies) - current_cookie_names

            if not missing_cookies:
                config.logger.info("Все требуемые куки найдены!")
                return

            config.logger.debug(f"Ожидаем куки: {missing_cookies}")
            time.sleep(1)

        config.logger.warning(f"Таймаут ожидания кук")

    def _transfer_cookies_to_requests(self, selenium_cookies: list, url: str):
        """Перенос кук из Selenium в requests.Session"""
        domain = self._extract_domain(url)

        transferred_count = 0
        for cookie in selenium_cookies:
            try:
                requests_cookie = requests.cookies.create_cookie(
                    name=cookie['name'],
                    value=cookie['value'],
                    domain=cookie.get('domain', domain),
                    path=cookie.get('path', '/'),
                    secure=cookie.get('secure', False),
                    rest={'HttpOnly': cookie.get('httpOnly', False)}
                )
                self.session.cookies.set_cookie(requests_cookie)
                transferred_count += 1
                config.logger.debug(f"Перенесена кука: {cookie['name']}")

            except Exception as e:
                config.logger.warning(f"Не удалось перенести куку {cookie['name']}: {e}")

        config.logger.info(f"Перенесено {transferred_count}/{len(selenium_cookies)} кук в requests сессию")

    def _extract_domain(self, url: str) -> str:
        """Извлечение домена из URL"""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return parsed.netloc

    def get_requests_session(self):
        """Получить requests сессию с куками"""
        return self.session

    def close(self):
        """Закрытие драйвера"""
        if self.driver:
            self.driver.quit()
            config.logger.info("Chrome драйвер закрыт")

def init_session_with_cookies(url: str, wait_for_cookies: list) -> requests.Session:
    """Инициализируем сессию с cookies"""
    config.logger.info("Инициализируем сессию с cookies")

    selenium_manager = SeleniumCookieManager()
    selenium_manager.setup_driver()
    selenium_manager.get_cookies_with_selenium(
        url=url,
        wait_for_cookies=wait_for_cookies
    )
    session = selenium_manager.get_requests_session()
    selenium_manager.close()

    return session
