# Внешние зависимости
from aiogram import Router
# Внутренние модули
from app.bot.handlers.admin import router as admin_router
from app.bot.handlers.files import router as file_router


router = Router()
router.include_router(admin_router)
router.include_router(file_router)