import logging
import os
import time
from typing import List, Tuple

import psycopg2
from dotenv import load_dotenv

from db_manager import DBManager
from db_utils import create_database, create_tables
from hh_api import Hh_Api

load_dotenv()  # Загрузка переменных из .env

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", filename="app.log")


def validate_data(vacancy: dict) -> bool:
    """Проверяет корректность данных вакансии"""
    required_fields = ["id", "name", "employer"]
    for field in required_fields:
        if field not in vacancy:
            logging.warning(f"Отсутствует обязательное поле {field} в вакансии {vacancy.get('id', 'N/A')}")
            return False
    return True


def get_vacancies_with_retry(api: Hh_Api, employer_id: int, retries: int = 3) -> list[dict]:
    """Получает вакансии с повторными попытками"""
    for attempt in range(retries + 1):
        try:
            return api.get_vacancies(employer_id)
        except Exception as e:
            if attempt < retries:
                wait_time = 2**attempt
                logging.warning(f"Повторная попытка ({attempt + 1}/{retries}) через {wait_time} сек...")
                time.sleep(wait_time)
            else:
                logging.error(f"Не удалось получить вакансии для {employer_id}: {str(e)}")
    return []


def insert_employers(data: list[dict], conn: psycopg2.extensions.connection) -> None:
    """Вставляет данные о работодателях в таблицу employers"""
    with conn.cursor() as cursor:
        for employer in data:
            try:
                cursor.execute(
                    """
                    INSERT INTO employers (employer_id, name, url)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (employer_id) DO NOTHING;
                    """,
                    (employer.get("id"), employer.get("name", ""), employer.get("alternate_url", "")),
                )
            except Exception as e:
                logging.error(f"Ошибка при вставке работодателя {employer.get('id', 'N/A')}: {str(e)}")
    conn.commit()


def insert_vacancies(data: list[dict], conn: psycopg2.extensions.connection) -> None:
    """Вставляет данные о вакансиях в таблицу vacancies"""
    with conn.cursor() as cursor:
        for vacancy in data:
            if not validate_data(vacancy):
                continue
            employer = vacancy.get("employer", {})
            salary = vacancy.get("salary", {})
            if not isinstance(salary, dict):
                salary = {}

            try:
                cursor.execute(
                    """
                    INSERT INTO vacancies (
                        vacancy_id, title, salary_from, salary_to, currency, employer_id, url
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (vacancy_id) DO NOTHING;
                    """,
                    (
                        vacancy.get("id", None),
                        vacancy.get("name", ""),
                        salary.get("from"),
                        salary.get("to"),
                        salary.get("currency"),
                        employer.get("id", None),
                        vacancy.get("alternate_url", ""),
                    ),
                )
            except Exception as e:
                logging.error(f"Ошибка при вставке вакансии {vacancy.get('id', 'N/A')}: {str(e)}")
    conn.commit()


def main() -> None:
    try:
        employer_ids = [
            "1942330",
            "49357",
            "3036416",
            "78638",
            "2748",
            "1740",
            "3529",
            "23427",
            "3772",
            "15478",
            "1122462",
        ]
        employer_ids = [id.strip() for id in employer_ids if id.strip()]

        api = Hh_Api()
        db_params = {
            "host": os.getenv("DB_HOST", "localhost"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", ""),
            "database": os.getenv("DB_NAME", "hh_db"),
        }

        # Создаем БД и таблицы
        create_database(
            db_params["database"],
            {"host": db_params["host"], "user": db_params["user"], "password": db_params["password"]},
        )
        create_tables(db_params["database"], db_params)

        # Получаем данные о работодателях
        employers_data = []
        for employer_id_str in employer_ids:
            employer_id = int(employer_id_str)  # Преобразуем в int
            try:
                employer = api.get_employer(employer_id)
                employers_data.append(employer)
            except Exception as e:
                logging.error(f"Ошибка при получении данных о работодателе {employer_id}: {str(e)}")

        # Получаем данные о вакансиях
        vacancies_data = []
        for employer_id_str in employer_ids:
            employer_id = int(employer_id_str)
            vacancies = get_vacancies_with_retry(api, employer_id)
            vacancies_data.extend(vacancies)

        # Загружаем данные в БД
        with psycopg2.connect(
            host=db_params["host"],
            user=db_params["user"],
            password=db_params["password"],
            database=db_params["database"],
        ) as conn:
            insert_employers(employers_data, conn)
            insert_vacancies(vacancies_data, conn)

        print("Данные успешно загружены в БД")
    except psycopg2.Error as e:
        logging.critical(f"Ошибка БД: {str(e)}")
    except Exception as e:
        logging.critical(f"Неизвестная ошибка: {str(e)}")


def user_interface() -> None:
    try:
        # Получаем параметры из .env
        db_params = {
            "host": os.getenv("DB_HOST", "localhost"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", ""),
            "database": os.getenv("DB_NAME", "hh_db"),
        }
        db = DBManager(db_params)

        while True:
            print("\nВыберите действие:")
            print("1. Компании и количество вакансий")
            print("2. Все вакансии")
            print("3. Средняя зарплата")
            print("4. Вакансии с зарплатой выше средней")
            print("5. Поиск вакансий по ключевому слову")
            print("0. Выход")

            choice = input("Введите номер действия: ")

            try:
                if choice == "1":
                    companies = db.get_companies_and_vacancies_count()
                    for company in companies:
                        print(f"Компания: {company[0]}, Вакансий: {company[1]}")
                elif choice == "2":
                    vacancies = db.get_all_vacancies()
                    for vacancy in vacancies:
                        if len(vacancy) >= 6:
                            print(
                                f"Компания: {vacancy[0]}, Вакансия: {vacancy[1]}, "
                                f"Зарплата: {vacancy[2]}-{vacancy[3]} {vacancy[4]}, URL: {vacancy[5]}"
                            )
                        else:
                            print("Неполные данные вакансии")
                elif choice == "3":
                    avg_salary = db.get_avg_salary()
                    if avg_salary and avg_salary[0] and len(avg_salary[0]) > 0:
                        print(f"Средняя зарплата: {avg_salary[0][0]}")
                    else:
                        print("Данные о зарплатах отсутствуют")
                elif choice == "4":
                    higher_salary_vacancies: List[Tuple[str, float]] = db.get_vacancies_with_higher_salary()
                    for item in higher_salary_vacancies:  # Используем другое имя переменной
                        if len(item) >= 2:
                            print(f"Вакансия: {item[0]}, Зарплата: {item[1]}")
                        else:
                            print("Неполные данные вакансии")
                elif choice == "5":
                    keyword = input("Введите ключевое слово: ")
                    keyword_vacancies: List[Tuple[str, int, str]] = db.get_vacancies_with_keyword(keyword)
                    for entry in keyword_vacancies:  # Используем другое имя переменной
                        if len(entry) >= 3:
                            print(f"Вакансия: {entry[0]}, Компания: {entry[1]}, URL: {entry[2]}")
                        else:
                            print("Неполные данные вакансии")
                elif choice == "0":
                    break
                else:
                    print("Неверный выбор. Попробуйте снова.")
            except Exception as e:
                logging.error(f"Ошибка при выполнении запроса: {str(e)}")
                print("Произошла ошибка. Попробуйте снова.")
    except Exception as e:
        logging.critical(f"Критическая ошибка интерфейса: {str(e)}")


if __name__ == "__main__":
    main()
    try:
        user_interface()
    except KeyboardInterrupt:
        logging.info("Программа остановлена пользователем")
