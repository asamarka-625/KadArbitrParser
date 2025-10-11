# Внешние зависимости
from typing import Optional
import requests
# Внутренние модули
from app.settings.config import get_config


config = get_config()


class ParserAddress:
    def __init__(self):
        pass

    @staticmethod
    def get_info_for_address(address: str) -> Optional[dict]:
        url = (f"https://catalog.api.2gis.com/3.0/items/geocode?q={address}&"
               f"fields=items.adm_div&key={config.GIS_KEY}")
        config.logger.info(f"Делаем запрос: {url}")
        
        try:
            response = requests.get(url)
            response.raise_for_status()

            return response.json()["result"]

        except requests.HTTPError as err:
            config.logger.error(f"Ошибка запроса к 2GIS. HTTPError: {err}")

        except Exception as err:
            config.logger.error(f"Ошибка запроса к 2GIS. Error: {err}")

    @staticmethod
    def get_district(info: dict) -> Optional[str]:
        for item in info["items"]:
            if (item.get("adm_div") is not None 
                and len(item["adm_div"]) >= 4 
                and item["adm_div"][3].get("name") is not None):
                    return item["adm_div"][3]["name"]
        
        return None
            
    def run(self, address: str) -> Optional[str]:
        config.COUNT_USED_GIS_KEY += 1
        result = self.get_info_for_address(address=address)
        if result is None:
            return None

        district = self.get_district(result)

        return district