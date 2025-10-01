from typing import Any, Dict, List, Tuple, cast

import psycopg2


class DBManager:
    """Класс для управления данными в БД

    Args:
        params (Dict[str, str]): Параметры подключения к PostgreSQL (host, user, password, database).
    """

    def __init__(self, params: Dict[str, str]):
        self.params = params

    def get_companies_and_vacancies_count(self) -> List[Tuple[str, int]]:
        """Получает список всех компаний и количество вакансий у каждой компании.

        Returns:
            List[Tuple[str, int]]: Список кортежей (название компании, количество вакансий).
        """
        query = """
            SELECT e.name, COUNT(v.id) AS vacancy_count
            FROM employers e
            LEFT JOIN vacancies v ON e.employer_id = v.employer_id
            GROUP BY e.name;
        """
        return cast(List[Tuple[str, int]], self._execute_query(query))

    def get_all_vacancies(self) -> List[Tuple[str, str, int, int, str, str]]:
        """Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию

        Returns:
            List[Tuple[str, str, int, int, str, str]]:
                Список кортежей (название компании, название вакансии,
                зарплата от, зарплата до, валюта, URL вакансии).
        """
        query = """
            SELECT
                e.name AS employer_name,
                v.title,
                v.salary_from,
                v.salary_to,
                v.currency,
                v.url
            FROM vacancies v
            JOIN employers e ON v.employer_id = e.employer_id;
        """
        return cast(List[Tuple[str, str, int, int, str, str]], self._execute_query(query))

    def get_avg_salary(self) -> List[Tuple[float]]:
        """Получает среднюю зарплату по всем вакансиям.

        Returns:
            List[Tuple[float]]: Средняя зарплата в виде списка с одним элементом.
        """
        query = """
            SELECT ROUND(AVG((salary_from + salary_to) / 2)) AS avg_salary
            FROM vacancies
            WHERE salary_from IS NOT NULL AND salary_to IS NOT NULL;
        """
        result = self._execute_query(query)
        return cast(List[Tuple[float]], result)

    def get_vacancies_with_higher_salary(self) -> List[Tuple[str, float]]:
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.

        Returns:
            List[Tuple[str, float]]:
                Список кортежей (название вакансии, средняя зарплата).
        """
        query = """
            SELECT
                title,
                (salary_from + salary_to) / 2 AS avg_salary
            FROM vacancies
            WHERE
                (salary_from + salary_to) / 2 > (

                    SELECT AVG((salary_from + salary_to) / 2)
                    FROM vacancies
                )
                AND salary_from IS NOT NULL
                AND salary_to IS NOT NULL;
        """
        return cast(List[Tuple[str, float]], self._execute_query(query))

    def get_vacancies_with_keyword(self, keyword: str) -> List[Tuple[str, int, str]]:
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова.

        Args:
            keyword (str): Ключевое слово для поиска (например, "Python").

        Returns:
            List[Tuple[str, int, str]]:
                Список кортежей (название вакансии, ID работодателя, URL).
        """
        # Используем параметризованный запрос для безопасности
        query = """
            SELECT title, employer_id, url
            FROM vacancies
            WHERE LOWER(title) LIKE %s;
        """
        with psycopg2.connect(
            host=self.params["host"],
            user=self.params["user"],
            password=self.params["password"],
            database=self.params["database"],
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (f"%{keyword.lower()}%",))
                return cast(List[Tuple[str, int, str]], cursor.fetchall())

    def _execute_query(self, query: str) -> List[Tuple[Any, ...]]:
        """Выполняет SQL-запрос и возвращает результат

        Args:
            query (str): SQL-запрос.

        Returns:
            List[Tuple[Any, ...]]: Результат выполнения запроса.
        """
        with psycopg2.connect(
            host=self.params["host"],
            user=self.params["user"],
            password=self.params["password"],
            database=self.params["database"],
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
