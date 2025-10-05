# Внешние зависимости
import asyncio
import threading
import time
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
# Внутренние модули
from app.scheduler.task_scheduler import TaskScheduler
from app.settings.config import get_config
from app.bot.handlers import router


config = get_config()


class BotManager:
    def __init__(self):
        self.bot = None
        self.dp = None
        self.scheduler = None
        self._notification_queue = asyncio.Queue()
        self._notification_task = None
    
    async def init_bot(self, token: str):
        """Инициализация бота"""
        self.bot = Bot(
            token=token, 
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
        self.dp = Dispatcher()
        
        # Регистрация роутеров
        self.dp.include_router(router)
        
        # Запускаем обработчик уведомлений
        self._notification_task = asyncio.create_task(self._notification_worker())
        
        return self.bot, self.dp
    
    async def _notification_worker(self):
        """Рабочий процесс для отправки уведомлений"""
        while True:
            try:
                message = await self._notification_queue.get()
                if message is None:  # Сигнал остановки
                    break
                    
                await self._safe_send_message(message)
                self._notification_queue.task_done()
                
            except Exception as e:
                config.logger.error(f"Ошибка в worker уведомлений: {e}")
                await asyncio.sleep(1)
    
    async def _safe_send_message(self, message: str):
        """Безопасная отправка сообщения"""
        try:
            # Создаем новую сессию для каждого сообщения
            async with self.bot.context() as bot:
                await bot.send_message(config.ADMIN_ID, message)
                config.logger.info(f"Уведомление отправлено: {message}")
                
        except Exception as e:
            config.logger.error(f"Ошибка отправки уведомления: {e}")
            
    async def send_notification(self, message: str):
        """Отправка уведомления администратору через очередь"""
        if not self.bot or not config.ADMIN_ID:
            return
            
        try:
            await self._notification_queue.put(message)
            config.logger.info(f"Уведомление добавлено в очередь: {message}")
            
        except Exception as e:
            config.logger.error(f"Ошибка добавления уведомления в очередь: {e}")
            
    def start_scheduler_in_thread(self, task, loop):
        """Запуск планировщика в отдельном потоке"""
        def run_scheduler():
            """Функция для запуска в потоке"""
            try:
                # Создаем новый цикл событий для этого потока
                self.scheduler_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.scheduler_loop)
                
                # Создаем планировщик в этом потоке
                self.scheduler = TaskScheduler(bot_manager=self, loop=loop)
                self.scheduler.start()
                
                # Добавляем задачу
                self.scheduler.add_hourly_job(
                    "main_task",
                    task,
                    timeout_minutes=config.TIMEOUT_WORK_MINUTES,
                    range_days=config.RANGE_DAYS_WORK,
                    delta_days=config.DELTA_DAYS_WORK,
                    file_path="data.json"
                )
                
                config.logger.info("Планировщик запущен в отдельном потоке")
                
                # Запускаем цикл событий
                self.scheduler_loop.run_forever()
                
            except Exception as e:
                config.logger.error(f"Ошибка в потоке планировщика: {e}")
                
            finally:
                if self.scheduler_loop and self.scheduler_loop.is_running():
                    self.scheduler_loop.stop()
        
        # Запускаем планировщик в отдельном потоке
        self.scheduler_thread = threading.Thread(
            target=run_scheduler,
            name="SchedulerThread",
            daemon=True
        )
        self.scheduler_thread.start()

    async def run_task_now(self, task_name, func, *args, **kwargs):
        """Запуск задачи немедленно через планировщик"""
        if not self.scheduler:
            await self.send_notification("❌ Планировщик не инициализирован")
            return False
        
        return await self.scheduler.run_task_now(task_name, func, *args, **kwargs)

    async def stop_manual_task(self, task_name, loop):
        """Остановка ручной задачи"""
        if not self.scheduler:
            return False
        
        return asyncio.run_coroutine_threadsafe(self.scheduler.stop_manual_task(task_name), loop)
        
    async def stop_all_manual_tasks(self, loop):
        """Остановка ручной задачи"""
        if not self.scheduler:
            return False
        
        return asyncio.run_coroutine_threadsafe(self.scheduler.stop_all_manual_tasks(), loop)

    async def get_system_status(self):
        """Получить статус системы"""
        if not self.scheduler:
            return {
                'scheduler_running': False,
                'manual_tasks_running': [],
                'scheduled_tasks_running': [],
                'scheduled_jobs_count': 0,
                'scheduled_jobs': []
            }
        
        return self.scheduler.get_system_status()

    def stop_scheduler(self):
        """Остановка планировщика"""
        if self.scheduler:
            # Запускаем shutdown в правильном event loop
            if self.scheduler_loop and self.scheduler_loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    self._async_shutdown(), 
                    self.scheduler_loop
                )
                try:
                    future.result(timeout=10)  # Ждем до 10 секунд
                except Exception as e:
                    config.logger.error(f"Ошибка при остановке планировщика: {e}")
            
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=5)

    async def stop(self):
        """Полная остановка бота и планировщика"""
        # Останавливаем планировщик
        if self.scheduler:
            self.scheduler.shutdown()
        
        # Останавливаем worker уведомлений
        if self._notification_task:
            await self._notification_queue.put(None)  # Сигнал остановки
            self._notification_task.cancel()
            try:
                await self._notification_task
            except asyncio.CancelledError:
                pass
        
        # Закрываем бота
        if self.bot:
            await self.bot.session.close()


_instance = None

def get_bot_manager() -> BotManager:
    global _instance
    if _instance is None:
        _instance = BotManager()
        
    return _instance
