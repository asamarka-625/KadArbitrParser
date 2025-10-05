# Внешние зависимости
import logging
import logging.handlers
import sys
from pathlib import Path


def setup_logger(
        name: str = __name__,
        level: str = "INFO",
        format_str: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        log_file: str = None,
        file_mode: str = 'a',
        max_bytes: int = 10 * 1024 * 1024,  # 10 MB
        backup_count: int = 5
) -> logging.Logger:
    logger = logging.getLogger(name)

    # Уровень
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(numeric_level)

    # Проверяем, не добавлены ли уже обработчики (чтобы избежать дублирования)
    if logger.handlers:
        return logger

    # Форматтер
    formatter = logging.Formatter(format_str)

    # Обработчик для stdout
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    stdout_handler.setLevel(numeric_level)
    logger.addHandler(stdout_handler)

    # Обработчик для файла (если указан)
    if log_file:
        try:
            # Создаем директорию если не существует
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            # RotatingFileHandler для ротации логов
            file_handler = logging.handlers.RotatingFileHandler(
                filename=log_file,
                mode=file_mode,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            file_handler.setLevel(numeric_level)
            logger.addHandler(file_handler)

            logger.info(f"Логирование в файл: {log_file}")

        except Exception as e:
            logger.error(f"Ошибка настройки файлового логгера: {e}")

    # Запрещаем propagation к корневому логгеру
    logger.propagate = False

    return logger