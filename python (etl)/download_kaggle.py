import os
import shutil
import kaggle

def download_kaggle_data(dataset_name, download_path='./data', target_path=r'C:\Users\Keno\Desktop\Sales-Data-Warehouse'):
    """Lädt ein Dataset von Kaggle herunter, entpackt es und verschiebt es in das Zielverzeichnis."""

    # Sicherstellen, dass der Zielordner für den Download existiert
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    # Kaggle Dataset herunterladen und entpacken
    kaggle.api.dataset_download_files(dataset_name, path=download_path, unzip=True)
    print(f"Dataset '{dataset_name}' erfolgreich heruntergeladen nach {download_path}")

    # Sicherstellen, dass das Zielverzeichnis existiert
    if not os.path.exists(target_path):
        os.makedirs(target_path)

    # Dateien ins Zielverzeichnis kopieren
    for file_name in os.listdir(download_path):
        full_file_name = os.path.join(download_path, file_name)
        if os.path.isfile(full_file_name):
            shutil.move(full_file_name, os.path.join(target_path, file_name))

    print(f"Dateien nach {target_path} verschoben.")

# Dataset herunterladen und verschieben

sicherheitsmechanismus = input("Gebe 'Confirm' ein, um das erneute Herunterladen zu bestätigen: ")

if sicherheitsmechanismus == "Confirm":
    download_kaggle_data('psparks/instacart-market-basket-analysis')

