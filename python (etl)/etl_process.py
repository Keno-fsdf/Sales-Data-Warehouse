import kaggle
import os
import pandas as pd

def download_kaggle_data(dataset_name, download_path='./data'):
    # Sicherstellen, dass der Zielordner existiert
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    # Dataset von Kaggle herunterladen
    kaggle.api.dataset_download_files(dataset_name, path=download_path, unzip=True)
    print(f"Dataset '{dataset_name}' erfolgreich heruntergeladen nach {download_path}")






#LOAD


# Funktion zum Laden der CSV-Dateien in Pandas DataFrames
def load_csv_files(data_path='./data/instacart-market-basket-analysis'):
    """
    Diese Funktion lädt mehrere CSV-Dateien aus dem angegebenen Verzeichnis
    und speichert sie als Pandas DataFrames in einem Dictionary.

    Parameter:
    - data_path (str): Der Dateipfad, in dem sich die CSV-Dateien befinden.
                       Standardmäßig: './data/instacart-market-basket-analysis'

    Rückgabe:
    - data (dict): Ein Dictionary, das die geladenen DataFrames enthält.
                   Schlüssel = Tabellenname (z. B. "orders"), 
                   Wert = Pandas DataFrame mit den CSV-Daten.
    """

    # Dictionary mit Dateinamen, die geladen werden sollen
    files = {
        "orders": "orders.csv",  # Enthält Informationen zu den Bestellungen
        "order_products_prior": "order_products__prior.csv",  # Artikel aus vorherigen Bestellungen
        "order_products_train": "order_products__train.csv",  # Artikel aus aktuellen Bestellungen
        "products": "products.csv",  # Produktliste mit IDs und Namen
        "aisles": "aisles.csv",  # Informationen zu den Gängen (z. B. "Gemüse", "Getränke")
        "departments": "departments.csv"  # Informationen zu Abteilungen (z. B. "Frischeprodukte")
    }
    
    # Leeres Dictionary für die geladenen DataFrames
    data = {}

    # Iteriere über das files-Dictionary, um alle CSV-Dateien zu laden
    for key, filename in files.items():  
        """
        Die Methode `.items()` gibt sowohl den Schlüssel (key) als auch den 
        zugehörigen Wert (filename) für jedes Element im Dictionary `files` zurück.

        Beispiel:
        - key = "orders", filename = "orders.csv"
        - key = "products", filename = "products.csv"
        """

        # Erstelle den vollständigen Pfad zur Datei (z. B. "./data/instacart-market-basket-analysis/orders.csv")
        file_path = os.path.join(data_path, filename)

        # Lade die CSV-Datei mit Pandas und speichere sie im `data`-Dictionary
        data[key] = pd.read_csv(file_path)

    # Erfolgsmeldung ausgeben
    print("✅ CSV-Dateien erfolgreich geladen.")

    # Das Dictionary mit den geladenen DataFrames zurückgeben
    return data




#Transform

def transform_data(data):
    # Beispiel: 'orders' bereinigen
    data["orders"].dropna(inplace=True)  # Null-Werte entfernen
    data["orders"]["order_id"] = data["orders"]["order_id"].astype(int)  # Datentyp anpassen

    # Beispiel: 'products' bereinigen
    data["products"].dropna(inplace=True)
    data["products"]["product_id"] = data["products"]["product_id"].astype(int)

    print("Daten erfolgreich transformiert.")
    return data







def etl_process():
    print("🔄 Starte ETL-Prozess...")
    
    # 1️⃣ EXTRACT – CSV-Dateien laden
    data = load_csv_files()

    # 2️⃣ TRANSFORM – Daten bereinigen
    transformed_data = transform_data(data)

    # 3️⃣ LOAD – Daten in MySQL speichern
    load_to_mysql(transformed_data)

    print("🎉 ETL-Prozess abgeschlossen!")
