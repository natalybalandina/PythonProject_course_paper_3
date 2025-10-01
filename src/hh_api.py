from typing import List

import requests


class Hh_Api:
    """Класс для получения данных с API hh.ru"""

    def __init__(self) -> None:
        self.base_url = "https://api.hh.ru"

    def get_employer(self, employer_id: int) -> dict:
        """Получить информацию о работодателе по ID"""
        url = f"{self.base_url}/employers/{employer_id}"
        response = requests.get(url)
        return response.json() if response.status_code == 200 else {}

    def get_vacancies(self, employer_id: int) -> List[dict]:
        """Получить список вакансий работодателя по ID"""
        url = f"{self.base_url}/vacancies"
        params = {"employer_id": employer_id}
        response = requests.get(url, params=params)
        return response.json().get("items", []) if response.status_code == 200 else []
