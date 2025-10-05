# Внешние зависимости
import asyncio
# Внутренние модули
from app.scheduler.worker import main_task
from app.bot.bot_manager import get_bot_manager
from app.settings.config import get_config


config = get_config()
bot_manager = get_bot_manager()


async def main():
    """Запуск бота и планировщика в разных потоках"""
    # Инициализация бота
    loop = asyncio.get_event_loop()
    bot, dp = await bot_manager.init_bot(config.BOT_TOKEN)
    
    # Сохраняем bot_manager в диспетчере для доступа из хэндлеров
    dp["bot_manager"] = bot_manager
    dp["loop"] = loop
    
    try:
        # Запускаем планировщик в отдельном потоке
        bot_manager.start_scheduler_in_thread(main_task, loop)
        
        # Ждем немного для инициализации планировщика
        await asyncio.sleep(2)
        
        await bot_manager.send_notification("🤖 Бот запущен! Планировщик работает в отдельном потоке.")
        config.logger.info("Система запущена в многопоточном режиме")
        
        # Запуск бота (основной поток)
        bot_task = asyncio.create_task(dp.start_polling(bot))
        
        # Мониторинг состояния системы
        async def system_monitor():
            """Мониторинг состояния системы"""
            while True:
                await asyncio.sleep(60)
                status = await bot_manager.get_system_status()
                
                running_manual = status['manual_tasks_running']
                running_scheduled = status['scheduled_tasks_running']
                
                if running_manual or running_scheduled:
                    config.logger.info(
                        f"Активные задачи: ручные={running_manual}, запланированные={running_scheduled}"
                    )
        
        monitor_task = asyncio.create_task(system_monitor())
        
        # Бесконечный цикл для основного потока
        config.logger.info("Скрипт запущен. Для остановки нажмите Ctrl+C")
        await asyncio.Future()  # Бесконечное ожидание
            
    except KeyboardInterrupt:
        await bot_manager.send_notification("🛑 Получен сигнал остановки...")
        config.logger.info("Получен сигнал остановки...")
        
    except Exception as e:
        await bot_manager.send_notification(f"❌ Критическая ошибка: {e}")
        config.logger.error(f"Критическая ошибка: {e}")
        
    finally:
        # Останавливаем планировщик
        bot_manager.stop_scheduler()
        
        # Отменяем задачи asyncio
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        
        # Ждем завершения задач
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Закрываем сессию бота
        await bot.session.close()
        
        await bot_manager.send_notification("✅ Приложение завершено")
        config.logger.info("Приложение завершено")
