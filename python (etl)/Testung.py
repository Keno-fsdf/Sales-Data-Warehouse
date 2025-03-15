import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

# Methode zur Verbindung mit der MySQL-Datenbank
def connect_to_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Apfelkuchen",
      
    )

# Funktion zur Bestimmung des SQL-Datentyps basierend auf dem Pandas-Datentyp
def get_sql_datatype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    else:
        return "VARCHAR(255)"

# Funktion, um automatisch eine Tabelle aus einer CSV-Datei zu erstellen
def create_table_from_csv(csv_file, table_name, db_cursor):
    # CSV-Datei einlesen
    df = pd.read_csv(csv_file)

    # SQL-Statement für die Tabellenerstellung
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("

    # Spaltennamen und Datentypen dynamisch bestimmen
    for column in df.columns:
        column_type = get_sql_datatype(df[column])  # Bestimme den SQL-Datentyp
        create_table_sql += f"{column} {column_type}, "

    # Entferne das letzte Komma und schließe die Klammer
    create_table_sql = create_table_sql.rstrip(", ") + ")"

    # SQL-Statement ausführen
    db_cursor.execute(create_table_sql)
    print(f"✅ Tabelle '{table_name}' erfolgreich erstellt.")

# Funktion zum Importieren von CSV-Dateien in die Datenbank
def import_csv_to_db(csv_file, table_name, engine):
    df = pd.read_csv(csv_file)

    # Daten in die MySQL-Datenbank laden (falls die Tabelle bereits existiert, wird sie ersetzt)
    df.to_sql(table_name, engine, index=False, if_exists='replace')

    print(f"✅ Tabelle '{table_name}' erfolgreich in die Datenbank geladen.")

# Hauptprogramm
if __name__ == "__main__":
    # Verbindung zur Datenbank
    with connect_to_db() as db:
        with db.cursor() as db_cursor:
            # CSV-Dateien und Tabellenzuordnungen
            csv_files = {
                'orders': 'orders.csv',
                'products': 'products.csv',
                'aisles': 'aisles.csv',
                'departments': 'departments.csv',
                'order_products_prior': 'order_products__prior.csv'
            }

            # Tabellen dynamisch erstellen basierend auf der Struktur der CSV-Dateien
            for table_name, csv_file in csv_files.items():
                create_table_from_csv(csv_file, table_name, db_cursor)

    # Jetzt verbinden wir uns mit SQLAlchemy für den CSV-Import
    engine = create_engine('mysql+mysqlconnector://root:Apfelkuchen@localhost:3306/sales_data_warehouse')

    # CSV-Dateien in die Datenbank importieren
    for table_name, csv_file in csv_files.items():
        import_csv_to_db(csv_file, table_name, engine)
