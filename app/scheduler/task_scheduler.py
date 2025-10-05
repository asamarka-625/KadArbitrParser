# Внешние зависимости
import time
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
# Внутренние модули
from app.settings.config import get_config


config = get_config()


class TaskScheduler:
    """Универсальный планировщик задач с управлением через aiogram"""
    
    def __init__(self, bot_manager=None, loop=None):
        self.scheduler = BackgroundScheduler()
        self.jobs = {}  # Задачи планировщика
        self.manual_tasks = {}  # Ручные задачи
        self.task_timeout = 300  # 5 минут по умолчанию
        self.executor = ThreadPoolExecutor(max_workers=5)
        self.bot_manager = bot_manager
        
        # Для отслеживания выполняющихся задач
        self.running_scheduled_tasks = {}  # Задачи от планировщика
        self.running_manual_tasks = {}     # Ручные задачи
        
        self._pending_notifications = []  # Очередь уведомлений до запуска event loop
        self._main_event_loop = loop
        self._stop_events = {}
        
    def start(self):
        """Запуск планировщика"""
        self.scheduler.start()
        config.logger.info("Планировщик задач запущен")
    
    def _send_pending_notifications(self):
        """Отправить накопившиеся уведомления"""
        if not self.bot_manager or not self._pending_notifications:
            return
            
        for message in self._pending_notifications:
            self._send_notification_async_safe(message)
        
        self._pending_notifications.clear()
        
    def shutdown(self):
        """Остановка планировщика"""
        try:
            self._stop_all_running_tasks()
            self.executor.shutdown(wait=False)
            
            if self.scheduler.running:
                self.scheduler.shutdown(wait=False)
                config.logger.info("Планировщик задач остановлен")
                self._send_notification_sync("🔴 Планировщик задач остановлен")
                
            else:
                config.logger.info("Планировщик уже остановлен")
                
        except Exception as err:
            config.logger.error(f"Ошибка при остановке планировщика: {err}")

    def _send_notification_async_safe(self, message: str):
        """Безопасная асинхронная отправка уведомления"""
        if not self.bot_manager:
            return
            
        try:
            if self._main_event_loop and self._main_event_loop.is_running():
                # Используем run_coroutine_threadsafe для потоков
                future = asyncio.run_coroutine_threadsafe(
                    self.bot_manager.send_notification(message),
                    self._main_event_loop
                )
                # Добавляем обработчик ошибок
                future.add_done_callback(
                    lambda f: self._handle_notification_result(f, message)
                )
            else:
                # Если event loop не доступен, просто логируем
                config.logger.warning(f"Event loop не доступен, уведомление не отправлено: {message}")
                
        except Exception as e:
            config.logger.error(f"Ошибка планирования уведомления: {e}")
    
    def _handle_notification_result(self, future, message: str):
        """Обработка результата отправки уведомления"""
        try:
            future.result()  # Проверяем результат
            config.logger.debug(f"Уведомление успешно отправлено: {message}")
        except Exception as e:
            config.logger.error(f"Ошибка при отправке уведомления '{message}': {e}")
    
    def _send_notification_sync(self, message: str):
        """Синхронная отправка уведомления из потоков планировщика"""
        config.logger.info(f"Уведомление: {message}")
        
        if not self.bot_manager:
            # Сохраняем уведомление для отправки позже
            self._pending_notifications.append(message)
            config.logger.debug(f"BotManager не установлен, уведомление сохранено: {message}")
            return
            
        # Пытаемся отправить уведомление
        self._send_notification_async_safe(message)
        
    async def _send_notification(self, message: str):
        """Отправка уведомления через бота"""
        if self.bot_manager:
            try:
                await self.bot_manager.send_notification(message)
                
            except Exception as e:
                config.logger.error(f"Ошибка отправки уведомления: {e}")

    def _run_with_timeout(self, func, job_id, *args, **kwargs):
        """Запуск функции с таймаутом и уведомлениями (для планировщика)"""
        def task_wrapper():
            try:
                config.logger.info(f"Запланированная задача '{job_id}' начата")
                
                # Уведомление о начале задачи
                if self.bot_manager:
                    self._send_notification_sync(f"🟡 Запланированная задача '{job_id}' начата")
                
                start_time = time.time()
                
                # Передаем stop_event в функцию задачи
                task_kwargs = kwargs.copy()
                task_kwargs['stop_event'] = stop_event
                task_kwargs['loop'] = self._main_event_loop
                
                result = func(*args, **kwargs)
                
                execution_time = time.time() - start_time
                
                config.logger.info(f"Запланированная задача '{job_id}' завершена за {execution_time:.2f} сек")
                
                # Уведомление об успешном завершении
                if self.bot_manager:
                    self._send_notification_sync(f"✅ Запланированная задача '{job_id}' завершена за {execution_time:.2f} сек")
                
                return result

            except Exception as e:
                config.logger.error(f"Ошибка в запланированной задаче '{job_id}': {e}")
                if self.bot_manager:
                    self._send_notification_sync(f"❌ Ошибка в запланированной задаче '{job_id}': {str(e)}")
                    
                raise

        try:
            # Запускаем задачу с таймаутом
            future = self.executor.submit(task_wrapper)
            self.running_scheduled_tasks[job_id] = {
                'future': future,
                'start_time': time.time(),
                'thread': threading.current_thread().ident
            }

            result = future.result(timeout=self.task_timeout)
            self.running_scheduled_tasks.pop(job_id, None)
            return result

        except TimeoutError:
            config.logger.error(f"Запланированная задача '{job_id}' превысила лимит времени ({self.task_timeout} сек)")
            
            if self.bot_manager:
                self._send_notification_sync(f"⏰ Запланированная задача '{job_id}' превысила лимит времени ({self.task_timeout} сек)")

            if job_id in self.running_scheduled_tasks:
                future = self.running_scheduled_tasks[job_id]['future']
                if not future.done():
                    future.cancel()
                self.running_scheduled_tasks.pop(job_id, None)
            
            return None

        except Exception as e:
            config.logger.error(f"Неожиданная ошибка в запланированной задаче '{job_id}': {e}")
            self.running_scheduled_tasks.pop(job_id, None)
            return None

    # Методы для запланированных задач
    def add_hourly_job(self, job_id, func, timeout_minutes=180, *args, **kwargs):
        """Добавление задачи на выполнение каждые 5 часов"""
        self.task_timeout = timeout_minutes * 60

        job = self.scheduler.add_job(
            self._run_with_timeout,
            trigger=CronTrigger(hour='*/5'),
            args=[func, job_id] + list(args),
            kwargs=kwargs,
            id=job_id,
            replace_existing=True,
            max_instances=1
        )

        self.jobs[job_id] = job
        config.logger.info(f"Запланированная задача '{job_id}' добавлена (каждые 5 часов)")
        
        if self.bot_manager:
            self._send_notification_sync(f"📅 Запланированная задача '{job_id}' добавлена (каждые 5 часов)")
    
    async def stop_manual_task(self, task_name):
        """Остановка ручной задачи с механизмом безопасной остановки"""
        if task_name not in self.running_manual_tasks:
            if self.bot_manager:
                await self.bot_manager.send_notification(f"❌ Задача '{task_name}' не найдена")
            return False
        
        task_info = self.running_manual_tasks[task_name]
        thread = task_info['thread']
        
        if not thread.is_alive():
            # Задача уже завершилась
            self.running_manual_tasks.pop(task_name)
            self._stop_events.pop(task_name, None)
            if self.bot_manager:
                await self.bot_manager.send_notification(f"ℹ️ Задача '{task_name}' уже завершена")
            return True
        
        try:
            # Устанавливаем флаг остановки
            if task_name in self._stop_events:
                self._stop_events[task_name].set()
            
            # Ожидаем завершения потока с таймаутом
            config.logger.info(f"Ожидание завершения задачи '{task_name}'...")
            thread.join(timeout=30)  # Ждем до 30 секунд
            
            if thread.is_alive():
                # Если поток все еще жив после таймаута
                config.logger.warning(f"Задача '{task_name}' не завершилась за 30 секунд, принудительная остановка")
                await self.bot_manager.send_notification(f"🛑 Требуется принудительная остановка задачи '{task_name}'...")
                        
            else:
                # Задача успешно завершена
                self.running_manual_tasks.pop(task_name)
                self._stop_events.pop(task_name, None)
                if self.bot_manager:
                    await self.bot_manager.send_notification(f"✅ Задача '{task_name}' успешно остановлена")
                return True
                
        except Exception as e:
            config.logger.error(f"Ошибка при остановке задачи '{task_name}': {e}")
            if self.bot_manager:
                await self.bot_manager.send_notification(f"❌ Ошибка остановки задачи '{task_name}': {e}")
                
            return False
            
    def stop_all_manual_tasks(self):
        """Остановка всех ручных задач"""
        tasks_to_stop = list(self.running_manual_tasks.keys())
        results = []
        
        for task_name in tasks_to_stop:
            try:
                # Создаем асинхронную задачу для остановки
                if self._main_event_loop and self._main_event_loop.is_running():
                    future = asyncio.run_coroutine_threadsafe(
                        self.stop_manual_task(task_name),
                        self._main_event_loop
                    )
                    results.append((task_name, future))
            except Exception as e:
                config.logger.error(f"Ошибка при планировании остановки задачи '{task_name}': {e}")
        
        return results
        
    def is_task_stopped(self, task_name):
        """Проверить, остановлена ли задача"""
        if task_name not in self.running_manual_tasks:
            return True
        
        task_info = self.running_manual_tasks[task_name]
        return not task_info['thread'].is_alive()
            
    # Методы для ручного управления задачами (из aiogram)
    async def run_task_now(self, task_name, func, *args, **kwargs):
        """Немедленный запуск задачи в отдельном потоке с поддержкой остановки"""
        try:
            if task_name in self.running_manual_tasks:
                task_info = self.running_manual_tasks[task_name]
                if task_info['thread'].is_alive():
                    if self.bot_manager:
                        await self.bot_manager.send_notification(f"⚠️ Ручная задача '{task_name}' уже выполняется")
                    return False
            
            # Создаем событие для остановки
            stop_event = threading.Event()
            self._stop_events[task_name] = stop_event
            
            def run_manual_task():
                try:
                    thread_id = threading.current_thread().ident
                    start_time = time.time()
                    
                    config.logger.info(f"Ручная задача '{task_name}' начата (Поток: {thread_id})")
                    self._send_notification_sync(f"🟡 Ручная задача '{task_name}' начата")
                    
                    # Передаем stop_event в функцию задачи
                    task_kwargs = kwargs.copy()
                    task_kwargs['stop_event'] = stop_event
                    task_kwargs['loop'] = self._main_event_loop
                    
                    # Выполняем функцию
                    result = func(*args, **task_kwargs)
                    
                    execution_time = time.time() - start_time
                    config.logger.info(f"Ручная задача '{task_name}' завершена (Поток: {thread_id}, время: {execution_time:.2f} сек)")
                    self._send_notification_sync(f"✅ Ручная задача '{task_name}' завершена за {execution_time:.2f} сек")
                    
                    # Сохраняем информацию о завершении
                    if task_name in self.running_manual_tasks:
                        self.running_manual_tasks[task_name]['completion_time'] = time.time()
                        self.running_manual_tasks[task_name]['execution_time'] = execution_time
                        self.running_manual_tasks[task_name]['result'] = result
                    
                    return result
                    
                except Exception as e:
                    execution_time = time.time() - start_time
                    config.logger.error(f"Ошибка в ручной задаче '{task_name}': {e}")
                    self._send_notification_sync(f"❌ Ошибка в ручной задаче '{task_name}': {str(e)}")
                    
                    # Сохраняем информацию об ошибке
                    if task_name in self.running_manual_tasks:
                        self.running_manual_tasks[task_name]['error'] = str(e)
                        self.running_manual_tasks[task_name]['execution_time'] = execution_time
                    
                    raise

            task_thread = threading.Thread(
                target=run_manual_task,
                name=f"ManualTask-{task_name}",
                daemon=True
            )
            
            # Сохраняем полную информацию о задаче
            self.running_manual_tasks[task_name] = {
                'thread': task_thread,
                'start_time': time.time(),
                'stop_event': stop_event,
                'func': func,
                'args': args,
                'kwargs': kwargs
            }
            
            task_thread.start()
            
            if self.bot_manager:
                await self.bot_manager.send_notification(f"🚀 Ручная задача '{task_name}' запущена")
            
            return True
            
        except Exception as e:
            config.logger.error(f"Ошибка запуска ручной задачи '{task_name}': {e}")
            # Очищаем ресурсы при ошибке запуска
            self._stop_events.pop(task_name, None)
            self.running_manual_tasks.pop(task_name, None)
            
            if self.bot_manager:
                await self.bot_manager.send_notification(f"❌ Ошибка запуска ручной задачи '{task_name}': {e}")
            return False

    # Методы для получения информации
    def get_running_manual_tasks_info(self):
        """Получить информацию о выполняющихся ручных задач"""
        running = []
        current_time = time.time()
        
        for task_name, task_info in self.running_manual_tasks.items():
            if task_info["thread"].is_alive():
                running.append(task_name)
        
        # Очищаем завершенные задачи
        completed = [name for name, task_info in self.running_manual_tasks.items() if not task_info["thread"].is_alive()]
        for name in completed:
            self.running_manual_tasks.pop(name, None)
            
        return running

    def get_running_scheduled_tasks_info(self):
        """Получить информацию о выполняющихся запланированных задачах"""
        info = []
        current_time = time.time()

        for job_id, task_info in self.running_scheduled_tasks.items():
            if not task_info['future'].done():
                execution_time = current_time - task_info['start_time']
                info.append({
                    'job_id': job_id,
                    'execution_time': round(execution_time, 2),
                    'status': 'running'
                })

        return info

    def get_all_jobs(self):
        """Список всех запланированных задач"""
        return self.scheduler.get_jobs()

    def get_system_status(self):
        """Получить полный статус системы"""
        running_manual = self.get_running_manual_tasks_info()
        running_scheduled = self.get_running_scheduled_tasks_info()
        scheduled_jobs = self.get_all_jobs()
        
        return {
            'scheduler_running': self.scheduler.running,
            'manual_tasks_running': running_manual,
            'scheduled_tasks_running': [task['job_id'] for task in running_scheduled],
            'scheduled_jobs_count': len(scheduled_jobs),
            'scheduled_jobs': [job.id for job in scheduled_jobs]
        }