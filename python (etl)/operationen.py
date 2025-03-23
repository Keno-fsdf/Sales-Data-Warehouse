import mysql.connector
from collections import defaultdict
import data_warehouse
import pandas as pd
import statistics
from openai import OpenAI 

def fetch_sales_data(db_cursor):
    query = """
    SELECT produkt_id, sales_id
    FROM Sales
    """
    db_cursor.execute(query)
    return db_cursor.fetchall()



##FUNKTIONIERT NOCH NICHT, da noch kein guthaben aufgeladen!!
def get_chatgpt_recommendation(data_string: str):
    """
    Diese Methode nimmt einen String (z.B. Analyseergebnisse) und gibt eine Antwort von ChatGPT zurück.

    :param data_string: Der String mit den Analyseergebnissen
    :return: Die Antwort von ChatGPT
    """

    # API-Schlüssel sicher aus Umgebungsvariable abrufen
    api_key = "key"

    if not api_key:
        raise ValueError("Der OpenAI API-Schlüssel wurde nicht gesetzt. Setze die Umgebungsvariable 'OPENAI_API_KEY'.")

    # OpenAI-Client mit API-Schlüssel initialisieren
    client = OpenAI(api_key=api_key)

    # Anfrage an OpenAI-API senden
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",  
        messages=[
            {"role": "system", "content": "Du bist ein hilfreicher Assistent."},
            {"role": "user", "content": data_string}
        ],
        temperature=0.7,
        max_tokens=200
    )

    # Antwort extrahieren und zurückgeben
    return response.choices[0].message.content.strip()
# Einkaufshistorie abrufen
def get_purchase_historys(db_cursor, table_name):
    print("Test 2")
    # data = data_warehouse.fetch_table_data(db_cursor, table_name)  # Holt die Datenbank-Tabelle als Pandas DataFrame -->die zeile ist dafür verantwortlich das word2vec augeruen wird
    data: pd.DataFrame = data_warehouse.fetch_table_data(db_cursor, table_name)
    print(f"Der Datentyp von 'data' ist: {type(data)}")

    #pd.set_option("display.max_rows", None)  # Keine Begrenzung für Zeilen beim anzeigen, also beim Print Statement, ansonsten wird das abgekürzt mit "..." ab einer gewissen menge
    #pd.set_option("display.max_columns", None)  # Keine Begrenzung für Spalten
    print(data)
    if data is None or data.empty:
        print("Keine Daten gefunden")   
        return []

   
    # Gruppiere die Produkte nach Bestellung
    grouped = data.groupby("order_id")["product_id"].apply(lambda x: x.astype(str).tolist())    #1. gruppiert alle Bestellungen, die die gleiche Bestellnummer haben und danach benutzten wir ein lambda, der dann die Produkt-IDs in Strings umwandelt und in eine Liste überführt
    
    return grouped.tolist()  #2. Es wird also eine Liste zurückgegeben

def support_details(product_support, num_to_display=3):
    result = ""

    # Berechne die Gesamtanzahl der Produkte
    total_products_count = len(product_support)

    # Höchsten und niedrigsten Support-Wert finden
    max_support = max(product_support.values())  # Höchster Support-Wert
    min_support = min(product_support.values())  # Niedrigster Support-Wert

    # Liste der Produkte mit dem höchsten und niedrigsten Support
    max_support_products = [product for product, support in product_support.items() if support == max_support]
    min_support_products = [product for product, support in product_support.items() if support == min_support]

    # Berechne den Median des Supports
    support_values = list(product_support.values())
    median_support = statistics.median(support_values)

    # Finde die Produkte, die den Median Support-Wert haben
    median_support_products = [product for product, support in product_support.items() if support == median_support]
    median_support_count = len(median_support_products)

    # Gesamtanzahl der Produkte
    result += f"Insgesamt gibt es {total_products_count} Produkte in der Liste.\n"

    result += f"Median Support: {median_support:.20f}\n"
    
    # Ausgabe der Anzahl der Produkte mit max/min Support
    result += f"Anzahl der Produkte mit dem höchsten Support ({max_support}): {len(max_support_products)}\n"
    result += f"Anzahl der Produkte mit dem niedrigsten Support ({min_support}): {len(min_support_products)}\n"
    result += f"Anzahl der Produkte mit dem Median Support ({median_support}): {median_support_count}\n"
    
    # Beispielhafte Produkte mit dem Median Support
    result += f"\nBeispielhafte Produkte mit dem Median Support:\n"
    for product in median_support_products[:num_to_display]:
        result += f"- {product} (Support: {median_support:.20f})\n"
    
    # Beispielhafte Produkte mit dem höchsten Support
    result += f"\nBeispielhafte Produkte mit dem höchsten Support:\n"
    for product in max_support_products[:num_to_display]:
        result += f"- {product} (Support: {max_support:.20f})\n"

    # Beispielhafte Produkte mit dem niedrigsten Support
    result += f"\nBeispielhafte Produkte mit dem niedrigsten Support:\n"
    for product in min_support_products[:num_to_display]:
        result += f"- {product} (Support: {min_support:.20f})\n"

    return result  # Rückgabe des gesamten Strings







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
    



    print("Total transactions (berechnet):", total_transactions)    


    print ("Der support ist:" )
    #print (product_support)
    support_data_chatgpt = support_details(product_support)
    #chatgpt_recoommmendation = get_chatgpt_recommendation( support_data_chatgpt)
    print(support_data_chatgpt)



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





"""
Anmerkung: Mir fällt gerade auf, dass die Datenmenge so nicht optimal ist.  -->support ist nur SINNVOLL BEI EINER GROßEN DATENMENGE
Ich müsste eine hohe Datenmenge auswählen, aber nur bei einem Produkt (oder einigen wenigen)  
den Support tatsächlich anzeigen lassen.  
 
Grund: Wenn die Datenmenge klein ist, kommt jedes Produkt (wahrscheinlich) nur einmal vor.  
Dadurch ist der Support für jedes Produkt gleich, weil jedes Produkt genau einmal vorkam.  

Lösung: Eine größere Datenmenge nutzen, damit Unterschiede im Support sichtbar werden.
Empfehlung: Eine Datenmenge von mind. 1000 Datensätzen.
-->Denke das gleiche gilt auch für Lift und Confidence
"""







def main():
    # Mit der Datenbank verbinden
    with data_warehouse.connect_to_db() as db:
        with db.cursor() as db_cursor:
            db_cursor.execute("Use retailsalesdw")


            # Verkaufsdaten abrufen
            sales_data = get_purchase_historys(db_cursor, "order_products__train")  # Hier wird nun die richtige Funktion aufgerufen.

            # Berechnung von Support, Confidence und Lift
            product_support, confidence_lift = calculate_support_confidence_lift(sales_data)
            db_cursor.fetchall()  # Holt alle Ergebnisse ab, um den Puffer zu leeren
            # Ergebnisse ausgeben
            # print_results(product_support, confidence_lift) -->DAS PRINT STATMENT Dann WIEDER WEG MACHEN


if __name__ == "__main__":
    main()
