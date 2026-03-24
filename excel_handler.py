import pandas as pd
import os
from typing import List, Dict, Any
from openpyxl import load_workbook

class ExcelHandler:
    COLUMNS = [
        "№", "Наименование предприятия", "ИНН", "Юр. форма", 
        "Изделия (несколько указывается через “;”)", "Группа контрагентов", 
        "ФИО руководителя", "Должность руководителя", "Адрес", 
        "Часовой пояс (от -12 до 12)", "Телефоны (несколько указывается через “;”) Формат номера, +7 (123) 456-78-90:комментарий", 
        "Сайт", "Почта для рассылки (несколько указывается через “;”)", 
        "Рубрика", "Подрубрика", "Заметки"
    ]

    @staticmethod
    def create_new(file_path: str, data: List[Dict[str, Any]]):
        df = pd.DataFrame(columns=ExcelHandler.COLUMNS)
        df = ExcelHandler._append_to_df(df, data, start_no=1)
        df.to_excel(file_path, index=False)

    @staticmethod
    def append_to_existing(file_path: str, data: List[Dict[str, Any]]):
        if not os.path.exists(file_path):
            ExcelHandler.create_new(file_path, data)
            return

        existing_df = pd.read_excel(file_path)
        
        # Ensure columns match exactly
        for col in ExcelHandler.COLUMNS:
            if col not in existing_df.columns:
                existing_df[col] = None
        
        # Determine last №
        last_no = 0
        if not existing_df.empty and "№" in existing_df.columns:
            try:
                last_no = int(existing_df["№"].max())
            except (ValueError, TypeError):
                last_no = len(existing_df)

        # Create a mapping of existing companies for easy lookup (by name or INN)
        # We normalize strings for better matching
        def normalize(s):
            return str(s).strip().lower() if pd.notnull(s) else ""

        def get_name_set(name):
            """Возвращает отсортированное множество слов для сравнения названий без учета порядка слов"""
            normalized = normalize(name)
            if not normalized:
                return set()
            # Убираем лишние символы, оставляем только буквы и цифры для сравнения слов
            import re
            words = re.findall(r'\w+', normalized)
            return set(words)

        for item in data:
            new_name = item.get("Наименование предприятия")
            new_inn = normalize(item.get("ИНН"))
            new_name_set = get_name_set(new_name)
            
            found_index = None
            
            # Search by INN first (if available)
            if new_inn:
                inn_matches = existing_df[existing_df["ИНН"].apply(normalize) == new_inn]
                if not inn_matches.empty:
                    found_index = inn_matches.index[0]
            
            # If not found by INN, search by fuzzy Name matching
            if found_index is None and new_name_set:
                # Проверяем каждое существующее название
                for idx, row in existing_df.iterrows():
                    existing_name = row.get("Наименование предприятия")
                    existing_name_set = get_name_set(existing_name)
                    
                    if existing_name_set == new_name_set:
                        found_index = idx
                        break

            if found_index is not None:
                # Update existing row with new non-null data
                mapped_row = ExcelHandler._map_item_to_row(item, existing_df.at[found_index, "№"])
                for col, val in mapped_row.items():
                    if col != "№" and val is not None and str(val).strip() != "":
                        existing_df.at[found_index, col] = val
            else:
                # Add as new row
                last_no += 1
                new_row = ExcelHandler._map_item_to_row(item, last_no)
                # Create a DataFrame for the new row with explicit columns to avoid FutureWarning
                new_row_df = pd.DataFrame([new_row], columns=ExcelHandler.COLUMNS)
                existing_df = pd.concat([existing_df, new_row_df], ignore_index=True)

        # Reorder columns to match template exactly
        existing_df = existing_df[ExcelHandler.COLUMNS]
        existing_df.to_excel(file_path, index=False)

    @staticmethod
    def _map_item_to_row(item: Dict[str, Any], no: int) -> Dict[str, Any]:
        return {
            "№": no,
            "Наименование предприятия": item.get("Наименование предприятия"),
            "ИНН": item.get("ИНН"),
            "Юр. форма": item.get("Юр. форма"),
            "Изделия (несколько указывается через “;”)": item.get("Изделия"),
            "Группа контрагентов": item.get("Группа контрагентов"),
            "ФИО руководителя": item.get("ФИО руководителя"),
            "Должность руководителя": item.get("Должность руководителя"),
            "Адрес": item.get("Адрес"),
            "Часовой пояс (от -12 до 12)": item.get("Часовой пояс"),
            "Телефоны (несколько указывается через “;”) Формат номера, +7 (123) 456-78-90:комментарий": item.get("Телефоны"),
            "Сайт": item.get("Сайт"),
            "Почта для рассылки (несколько указывается через “;”)": item.get("Почта для рассылки"),
            "Рубрика": item.get("Рубрика"),
            "Подрубрика": item.get("Подрубрика"),
            "Заметки": item.get("Заметки"),
        }

    @staticmethod
    def _append_to_df(df: pd.DataFrame, data: List[Dict[str, Any]], start_no: int) -> pd.DataFrame:
        if not data:
            return df
        rows = [ExcelHandler._map_item_to_row(item, start_no + i) for i, item in enumerate(data)]
        add_df = pd.DataFrame(rows, columns=ExcelHandler.COLUMNS)
        return pd.concat([df, add_df], ignore_index=True)
