import pandas as pd
from gensim.models import Word2Vec
import data_warehouse
import os  # Zum Überprüfen, ob die Datei existiert


# Einkaufshistorie abrufen
def get_purchase_history(db_cursor, table_name):
    data = data_warehouse.fetch_table_data(db_cursor, table_name)  # Holt die Datenbank-Tabelle als Pandas DataFrame

    if data is None or data.empty:
        print("Keine Daten gefunden")
        return []
    
    # Gruppiere die Produkte nach Bestellung
    grouped = data.groupby("order_id")["product_id"].apply(lambda x: x.astype(str).tolist())    #1. groupiert alle bestellungen die die gleiche Bestellnummer haben und danach benutzten wir ein lambda, der dann die product ids in STrings umwandelt und in eine liste überführt

    return grouped.tolist()  #2. Es wird also eine liste zurückgegeben

# Word2Vec-Modell trainieren
def train_word2vec(data):
    """
    Die Länge des erzeugten Vektors für jedes Wort/Produkt beträgt bspw. 50 Dimensionen. Höhere Werte können die Qualität der Darstellung verbessern, aber das Training verlangsamen.
    -->also quasi umso niedriger umso schneller, aber umso schlechtere qualität
    Das window gibt an wie viel Produkte rechts und links beachtet werden. Wenn dieser Wert zu niedrig ist, können simple zusammenhänge untergehen, wenn er zu groß ist, können komische Zusammenhänge gezogen werden.
    
    Produkte, die seltener als 2 mal in den Daten vorkommen, werden ignoriert. Das hilft, Rauschen zu reduzieren und unwichtige Daten auszuschließen.

    Anzahl der CPU-Kerne, die für das Training verwendet werden. Eine höhere Zahl beschleunigt das Training halt. Ist aber offensichtlich für mich.
    """
    model = Word2Vec(sentences=data, vector_size=50, window=5, min_count=2, workers=4)
    return model


# Modell speichern
def save_model(model, filename="word2vec_model"):
    model.save(filename)  # Speichert das Modell im angegebenen Dateinamen



# Modell laden
def load_model(filename="word2vec_model"):
    if os.path.exists(filename):  # Prüfen, ob die Datei existiert
        return Word2Vec.load(filename)
    else:
        print("Das Modell existiert nicht. Ein neues Modell wird trainiert.")
        return None


# Ähnliche Produkte finden
def find_similar_products(model, product_id, topn=5):
    try:
        similar_items = model.wv.most_similar(str(product_id), topn=topn)
        return similar_items
    except KeyError:
        return f"Produkt-ID {product_id} nicht im Modell gefunden."


# Hauptprogramm
if __name__ == "__main__":
    # Mit der Datenbank verbinden und den Cursor verwenden
    print("Lade Einkaufsdaten aus der Datenbank über data_warehouse.py...")

    # Verbindung und Cursor im with-Block öffnen
    with data_warehouse.connect_to_db() as db:  # Verbindung zur DB
        with db.cursor() as db_cursor:  # Cursor verwenden
            # Holen der Einkaufsdaten
            db_cursor.execute("USE retailsalesdw")
            purchase_history = get_purchase_history(db_cursor, "order_products__prior")

            if purchase_history:
                # Zuerst versuchen, das Modell zu laden
                model = load_model("word2vec_model")

                # Wenn das Modell nicht existiert, trainiere es
                if model is None:
                    print("Trainiere Word2Vec-Modell...")
                    model = train_word2vec(purchase_history)
                    # Modell speichern
                    save_model(model, "word2vec_model")

                # Beispiel: Ähnliche Produkte für Produkt-ID 24852 (Bananas)
                product_id = 24852
                print(f"Ähnliche Produkte zu {product_id}:")
                print(find_similar_products(model, product_id))
