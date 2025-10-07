# Внешние зависимости
from aiogram import Router, types
from aiogram.filters import Command
# Внутренние модули
from app.settings.config import get_config


config = get_config()
router = Router()


@router.message(Command("status"))
async def get_status(message: types.Message, bot_manager):
    """Получение статуса системы"""
    if not bot_manager:
        await message.answer("❌ BotManager не доступен")
        return
    
    status = await bot_manager.get_system_status()
    
    running_manual = status['manual_tasks_running']
    running_scheduled = status['scheduled_tasks_running']
    scheduled_jobs = status['scheduled_jobs']
    
    status_text = f"""
📊 Статус системы:

🤖 Бот: Работает
🧵 Планировщик: {'🟢 Запущен' if status['scheduler_running'] else '🔴 Остановлен'}
📅 Запланированные задачи: {len(scheduled_jobs)}

{'🔄 Ручные задачи: ' + ', '.join(running_manual) if running_manual else '✅ Нет ручных задач'}
{'🔄 Запланированные задачи: ' + ', '.join(running_scheduled) if running_scheduled else '✅ Нет активных запланированных задач'}
"""
    await message.answer(status_text)


@router.message(Command("run_now"))
async def run_task_now(message: types.Message, bot_manager):
    """Запуск задачи немедленно"""
    if message.from_user.id != config.ADMIN_ID:
        await message.answer("❌ У вас нет прав для выполнения этой команды")
        return
    
    await message.answer("🟡 Запуск основной задачи...")
    
    from app.scheduler.worker import main_task  # Импортируем здесь чтобы избежать циклических импортов
    
    success = await bot_manager.run_task_now(
        "manual_main_task",
        main_task,
        range_days=7,
        delta_days=2,
        file_path="data.json"
    )
    
    if not success:
        await message.answer("❌ Не удалось запустить задачу")


@router.message(Command("tasks"))
async def list_tasks(message: types.Message, bot_manager):
    """Список выполняющихся задач""" 
    status = await bot_manager.get_system_status()
    running_manual = status['manual_tasks_running']
    running_scheduled = status['scheduled_tasks_running']
    
    if running_manual or running_scheduled:
        tasks_text = ""
        if running_manual:
            tasks_text += "🔄 Ручные задачи:\n" + "\n".join([f"• {task}" for task in running_manual]) + "\n\n"
        if running_scheduled:
            tasks_text += "📅 Запланированные задачи:\n" + "\n".join([f"• {task}" for task in running_scheduled])
        
        await message.answer(tasks_text)
    else:
        await message.answer("✅ Нет выполняющихся задач")


@router.message(Command("stop_task"))
async def stop_task(message: types.Message, bot_manager, loop):
    """Остановка задачи"""
    if message.from_user.id != config.ADMIN_ID:
        await message.answer("❌ У вас нет прав для выполнения этой команды")
        return
    
    # Получаем имя задачи из аргументов
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("❌ Использование: /stop_task имя_задачи")
        return
    
    task_name = parts[1]
    success = await bot_manager.stop_manual_task(task_name, loop)
    
    if success:
        await message.answer(f"⏹️ Задача '{task_name}' помечена для остановки")
        
    else:
        await message.answer(f"❌ Задача '{task_name}' не найдена или не выполняется")
        

@router.message(Command("stop_all_tasks"))
async def stop_all_tasks(message: types.Message, bot_manager, loop):
    """Остановка всех задач"""
    if message.from_user.id != config.ADMIN_ID:
        await message.answer("❌ У вас нет прав для выполнения этой команды")
        return

    result = await bot_manager.stop_all_manual_tasks(loop)
    
    await message.answer(f"⏹️ Задачи остановлены: {result}")


@router.message(Command("gis_key_view"))
async def view_gis_key(message: types.Message):
    """Посмотреть ключ 2GIS"""
    if message.from_user.id != config.ADMIN_ID:
        await message.answer("❌ У вас нет прав для выполнения этой команды")
        return

    await message.answer(
        f"Текущий ключ 2GIS: {config.GIS_KEY}\n"
        f"Использовано раз: {config.COUNT_USED_GIS_KEY}"
    )

@router.message(Command("gis_key_update"))
async def update_gis_key(message: types.Message):
    """Обновить ключ 2GIS"""
    if message.from_user.id != config.ADMIN_ID:
        await message.answer("❌ У вас нет прав для выполнения этой команды")
        return

    # Получаем ключ из аргумента
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("❌ Использование: /gis_key_update ключ")
        return

    config.GIS_KEY = parts[1]
    config.COUNT_USED_GIS_KEY = 0

    await message.answer(f"Новый ключ 2GIS: {config.GIS_KEY}")


@router.message(Command("gis_key_used_update"))
async def update_count_used_gis_key(message: types.Message):
    """Обновить кол-во использований ключа 2GIS"""
    if message.from_user.id != config.ADMIN_ID:
        await message.answer("❌ У вас нет прав для выполнения этой команды")
        return

    # Получаем ключ из аргумента
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("❌ Использование: /gis_key_used_update число")
        return

    if not parts[1].isdigit():
        await message.answer("❌ Использование: /gis_key_used_update число")
        return

    config.COUNT_USED_GIS_KEY = int(parts[1])

    await message.answer(f"Использовано раз: {config.COUNT_USED_GIS_KEY}")