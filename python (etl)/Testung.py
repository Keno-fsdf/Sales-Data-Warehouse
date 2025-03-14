import os
import mysql.connector
import pandas as pd

# Verbindung zur MySQL-Datenbank herstellen
def connect_to_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Apfelkuchen"
       
    )

# Funktion zum Laden der CSV-Dateien
def load_csv_files(data_path='./data/instacart-market-basket-analysis'):
    files = {
        "orders": "orders.csv",
        "order_products_prior": "order_products__prior.csv",
        "order_products_train": "order_products__train.csv",
        "products": "products.csv",
        "aisles": "aisles.csv",
        "departments": "departments.csv"
    }

    data = {}
    for key, filename in files.items():
        file_path = os.path.join(data_path, filename)
        data[key] = pd.read_csv(file_path)

    print("CSV-Dateien erfolgreich geladen.")
    return data

# Transformation der Daten: Bereinigung, Formatierung und Berechnungen
def transform_data(data):
    # Beispiel für Datenbereinigung
    print("🔄 Starte Daten-Transformation...")

    # Entfernen von NaN-Werten in den 'days_since_prior_order' (ersetzen durch einen Default-Wert)
    data["orders"]["days_since_prior_order"].fillna(0, inplace=True)

    # Umwandlung von 'order_dow' (day of week) in string
    data["orders"]["order_dow"] = data["orders"]["order_dow"].astype(str)

    # Berechnung einer neuen Spalte "average_order_value" für den durchschnittlichen Bestellwert
    # Nehmen wir an, 'order_products_prior' enthält den Bestellwert
    avg_order_value = data["order_products_prior"].groupby("order_id")["product_id"].count()
    data["orders"]["average_order_value"] = data["orders"]["order_id"].map(avg_order_value)

    # Normalisierung von Preisangaben: Alle Preise sollen positive Werte haben
    data["products"]["preis"] = data["products"]["preis"].abs()

    # Weitere Transformationen wie z.B. das Entfernen von doppelten Einträgen
    data["order_products_prior"].drop_duplicates(inplace=True)
    data["order_products_train"].drop_duplicates(inplace=True)

    print("Daten erfolgreich transformiert.")
    return data

# Methode zur Erstellung des Data Warehouse (mit Instacart-Schema)
def create_data_warehouse(db, db_cursor):
    try:
        db_cursor.execute("BEGIN")

        # Erstellen der Datenbank, falls sie nicht existiert
        db_cursor.execute("CREATE DATABASE IF NOT EXISTS RetailSalesDW")
        db_cursor.execute("USE RetailSalesDW")

        # Neue Tabellen für Instacart-Daten
        table_queries = [
            """
            CREATE TABLE IF NOT EXISTS Orders (
                order_id INT PRIMARY KEY,
                user_id INT,
                eval_set VARCHAR(20),
                order_number INT,
                order_dow INT,
                order_hour_of_day INT,
                days_since_prior_order FLOAT,
                average_order_value FLOAT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS Order_Products_Prior (
                order_id INT,
                product_id INT,
                add_to_cart_order INT,
                reordered INT,
                PRIMARY KEY (order_id, product_id),
                FOREIGN KEY (order_id) REFERENCES Orders(order_id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS Order_Products_Train (
                order_id INT,
                product_id INT,
                add_to_cart_order INT,
                reordered INT,
                PRIMARY KEY (order_id, product_id),
                FOREIGN KEY (order_id) REFERENCES Orders(order_id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS Products (
                product_id INT PRIMARY KEY,
                product_name VARCHAR(255),
                aisle_id INT,
                department_id INT,
                preis DECIMAL(10,2)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS Aisles (
                aisle_id INT PRIMARY KEY,
                aisle VARCHAR(255)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS Departments (
                department_id INT PRIMARY KEY,
                department VARCHAR(255)
            )
            """
        ]

        for query in table_queries:
            db_cursor.execute(query)

        db.commit()
        print("Datenbank und Tabellen erfolgreich erstellt.")

    except Exception as e:
        db.rollback()
        print(f"Fehler beim Erstellen der Datenbank: {e}")

# Funktion zum Laden der Instacart-Daten in die Datenbank
def load_data_into_db(db, db_cursor, data):
    try:
        db_cursor.execute("BEGIN")

        # Orders-Tabelle füllen
        for _, row in data["orders"].iterrows():
            db_cursor.execute("""
                INSERT IGNORE INTO Orders (order_id, user_id, eval_set, order_number, order_dow, order_hour_of_day, days_since_prior_order, average_order_value)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))

        # Order_Products_Prior füllen
        for _, row in data["order_products_prior"].iterrows():
            db_cursor.execute("""
                INSERT IGNORE INTO Order_Products_Prior (order_id, product_id, add_to_cart_order, reordered)
                VALUES (%s, %s, %s, %s)
            """, tuple(row))

        # Order_Products_Train füllen
        for _, row in data["order_products_train"].iterrows():
            db_cursor.execute("""
                INSERT IGNORE INTO Order_Products_Train (order_id, product_id, add_to_cart_order, reordered)
                VALUES (%s, %s, %s, %s)
            """, tuple(row))

        # Products füllen
        for _, row in data["products"].iterrows():
            db_cursor.execute("""
                INSERT IGNORE INTO Products (product_id, product_name, aisle_id, department_id, preis)
                VALUES (%s, %s, %s, %s, %s)
            """, tuple(row))

        # Aisles füllen
        for _, row in data["aisles"].iterrows():
            db_cursor.execute("""
                INSERT IGNORE INTO Aisles (aisle_id, aisle)
                VALUES (%s, %s)
            """, tuple(row))

        # Departments füllen
        for _, row in data["departments"].iterrows():
            db_cursor.execute("""
                INSERT IGNORE INTO Departments (department_id, department)
                VALUES (%s, %s)
            """, tuple(row))

        db.commit()
        print("Daten erfolgreich in die Datenbank geladen.")

    except Exception as e:
        db.rollback()
        print(f"Fehler beim Laden der Daten: {e}")

# Hauptprogramm
if __name__ == "__main__":
    data = load_csv_files()  # Lade CSV-Daten
    transformed_data = transform_data(data)  # Transformiere die Daten

    with connect_to_db() as db:
        with db.cursor() as db_cursor:
            create_data_warehouse(db, db_cursor)  # Erstelle die Tabellen
            load_data_into_db(db, db_cursor, transformed_data)  # Lade transformierte Daten in die Datenbank
