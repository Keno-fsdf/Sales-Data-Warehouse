import mysql.connector
from collections import defaultdict

def connect_to_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Apfelkuchen",
        database="RetailSalesDW"
    )

def fetch_sales_data(db_cursor):
    query = """
    SELECT produkt_id, sales_id
    FROM Sales
    """
    db_cursor.execute(query)
    return db_cursor.fetchall()

def calculate_support_confidence_lift(sales_data):
    # Schritt 1: Berechnen des Supports für jedes Produkt
    product_support = defaultdict(int)
    total_transactions = len(sales_data)

    for sale in sales_data:
        product_support[sale[0]] += 1

    # Support für jedes Produkt
    product_support = {key: value / total_transactions for key, value in product_support.items()}

    # Schritt 2: Berechnen von Confidence und Lift für Paare
    product_pair_support = defaultdict(int)
    for sale in sales_data:
        for other_sale in sales_data:
            if sale[0] != other_sale[0]:
                pair = tuple(sorted([sale[0], other_sale[0]]))
                product_pair_support[pair] += 1

    # Confidence und Lift berechnen
    confidence_lift = {}
    for pair, pair_count in product_pair_support.items():
        product_a, product_b = pair
        support_a = product_support[product_a]
        support_b = product_support[product_b]

        # Confidence(A -> B)
        confidence_ab = pair_count / product_support[product_a]
        
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
    with connect_to_db() as db:
        with db.cursor() as db_cursor:
            # Verkaufsdaten abrufen
            sales_data = fetch_sales_data(db_cursor)

            # Berechnung von Support, Confidence und Lift
            product_support, confidence_lift = calculate_support_confidence_lift(sales_data)

            # Ergebnisse ausgeben
            print_results(product_support, confidence_lift)

if __name__ == "__main__":
    main()
