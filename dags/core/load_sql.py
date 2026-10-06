from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

def load_sql(relative_path: str) -> str:

    sql_file = BASE_DIR/ "scripts" / "sql" / relative_path

    if not sql_file:
        raise ValueError(f"Cannot find the {relative_path} file please check.")
    
    if not sql_file.exists():
        raise FileNotFoundError(
            f"Cannot find SQL file: {sql_file}"
        )

    return sql_file.read_text(encoding="utf-8")