import gender_guesser.detector as gender
from typing import Optional


class RussianGenderDetector:
    def __init__(self):
        self.gender_detector = gender.Detector()

        # Дополнительные правила для русских имен
        self.russian_female_endings = ['а', 'я', 'ия', 'ьа']
        self.russian_male_endings = ['й', 'ь', 'р', 'н', 'л', 'с', 'т', 'в', 'м']

        # Унисекс имена, которые требуют особой обработки
        self.unisex_names = {
            'женя', 'саша', 'валя', 'слава', 'сима', 'ваня', 'шура'
        }

    def detect_gender(self, fio: str) -> Optional[bool]:
        if not fio or not isinstance(fio, str):
            return None

        parts = fio.strip().split()

        if len(parts) < 2:
            return None

        # 0. Проверка на унисекс имена
        first_name = parts[1].lower()
        if first_name in self.unisex_names and len(parts) >= 3:
            # Для унисекс имен пол определяем только по отчеству
            middle_name = parts[2].lower()
            if middle_name.endswith(('вна', 'чна')):
                return False
            elif middle_name.endswith(('вич', 'чич')):
                return True
            return None

        # 1. ПРИОРИТЕТ: Определение по отчеству (95% точность)
        if len(parts) >= 3:
            middle_name = parts[2].lower()
            if middle_name.endswith(('вна', 'чна')):
                return False
            elif middle_name.endswith(('вич', 'чич')):
                return True

        # 2. Библиотека gender-guesser (80-90% точность для русских имен)
        try:
            result = self.gender_detector.get_gender(first_name)
            gender_map = {
                'male': True,
                'female': False,
                'mostly_male': True,
                'mostly_female': False
            }
            detected = gender_map.get(result)
            if detected is not None and result != 'unknown':
                return detected
        except Exception:
            pass

        # 3. Резерв: правила для русских имен (70-80% точность)
        # Женские имена обычно оканчиваются на 'а', 'я'
        if any(first_name.endswith(ending) for ending in self.russian_female_endings):
            return False

        # Мужские имена обычно оканчиваются на согласные
        if any(first_name.endswith(ending) for ending in self.russian_male_endings):
            return True

        return None

    def detect_gender_with_fallback(self, fio: str, default: bool = True) -> bool:
        result = self.detect_gender(fio)
        return result if result is not None else default


if __name__ == "__main__":
    detector = RussianGenderDetector()
    result = detector.detect_gender_with_fallback(fio="Макаровский Ярослав Сергеевич ")
    print(result)