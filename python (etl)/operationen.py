import mysql.connector
from collections import defaultdict
import data_warehouse
import pandas as pd
print("Moin")


def fetch_sales_data(db_cursor):
    query = """
    SELECT produkt_id, sales_id
    FROM Sales
    """
    db_cursor.execute(query)
    return db_cursor.fetchall()


# Einkaufshistorie abrufen
def get_purchase_historys(db_cursor, table_name):
    print("Test 2")
    #data = data_warehouse.fetch_table_data(db_cursor, table_name)  # Holt die Datenbank-Tabelle als Pandas DataFrame -->die zeile ist dafür verantwortlich das word2vec augeruen wird
    data: pd.DataFrame = data_warehouse.fetch_table_data(db_cursor, table_name)
    print(f"Der Datentyp von 'data' ist: {type(data)}")
    print(data)
    print("Test 3")
    if data is None or data.empty:
        print("Keine Daten gefunden")   
        return []

   
    # Gruppiere die Produkte nach Bestellung
    grouped = data.groupby("order_id")["product_id"].apply(lambda x: x.astype(str).tolist())    #1. gruppiert alle Bestellungen, die die gleiche Bestellnummer haben und danach benutzten wir ein lambda, der dann die Produkt-IDs in Strings umwandelt und in eine Liste überführt
    
    return grouped.tolist()  #2. Es wird also eine Liste zurückgegeben



def calculate_support_confidence_lift(sales_data):
    # Schritt 1: Berechnen des Supports für jedes Produkt
    # Ein defaultdict ist quasi eine Variable, die als Counter für mehrere Variablen (Produkte) verwendet wird.
    # Es setzt den Standardwert für neue Schlüssel auf 0 (weil int() standardmäßig 0 zurückgibt).
    product_support = defaultdict(int)
    total_transactions = len(sales_data)

    # Zählt wie oft jedes Produkt verkauft wurde
    for sale in sales_data:
        for product_id in sale:  # iteriert über jedes Produkt in der Transaktion (nicht nur sale[0])
            product_support[product_id] += 1

    # Support für jedes Produkt
    product_support = {key: value / total_transactions for key, value in product_support.items()}
    


    total_transactions = sum(product_support.values())  # Summe aller Einträge als Kontrollwert
    print("Total transactions (berechnet):", total_transactions)    


    print ("Der support ist:" )
    print (product_support)
    # Beispiel: "Apfel": 3 / 4 → 0.75    --> Falls "Apfel" 3-mal verkauft wurde und es 4 Transaktionen gibt.


    # Schritt 2: Berechnen von Confidence und Lift für Paare
    product_pair_support = defaultdict(int)   # Neues defaultdict für product_pair_support

    # Die zwei For-Loops sind wie zwei Pointer und vergleichen jedes Produkt mit jedem anderen.
    for sale in sales_data:
        for i in range(len(sale)):
            for j in range(i + 1, len(sale)):  # Vergleicht jedes Produkt mit allen anderen Produkten
                product_a = sale[i]
                product_b = sale[j]
                pair = tuple(sorted([product_a, product_b]))  # Sortiert die Produkt-IDs und stellt ein Paar dar
                product_pair_support[pair] += 1  # Zähler wird erhöht, also es gibt ein Paar, das zutrifft

    # Confidence und Lift berechnen
    confidence_lift = {}
    for pair, pair_count in product_pair_support.items():
        product_a, product_b = pair
        support_a = product_support[product_a]
        support_b = product_support[product_b]

        # Confidence(A -> B)
        confidence_ab = pair_count / support_a
        
        # Lift(A -> B)
        lift_ab = confidence_ab / support_b

        confidence_lift[pair] = {'confidence': confidence_ab, 'lift': lift_ab}

    return product_support, confidence_lift


def print_results(product_support, confidence_lift):
    print("Support für Produkte:")
    for product, support in product_support.items():
        print(f"Produkt {product}: {support:.4f}")
    
    print("\nConfidence und Lift für Produktpaare:")
    for pair, metrics in confidence_lift.items():
        product_a, product_b = pair
        print(f"Produktpaar ({product_a}, {product_b}):")
        print(f"  Confidence: {metrics['confidence']:.4f}")
        print(f"  Lift: {metrics['lift']:.4f}")


def main():
    # Mit der Datenbank verbinden
    with data_warehouse.connect_to_db() as db:
        with db.cursor() as db_cursor:
            db_cursor.execute("Use retailsalesdw")

            print("Testung")
            #Verkaufsdaten abrufen
            sales_data = get_purchase_historys(db_cursor, "order_products__train")  # **Geänderte Zeile**: Hier wird nun die richtige Funktion aufgerufen.

            #Berechnung von Support, Confidence und Lift
            product_support, confidence_lift = calculate_support_confidence_lift(sales_data)
            db_cursor.fetchall()  # Holt alle Ergebnisse ab, um den Puffer zu leeren
            # Ergebnisse ausgeben
            print_results(product_support, confidence_lift)


if __name__ == "__main__":
    main()
