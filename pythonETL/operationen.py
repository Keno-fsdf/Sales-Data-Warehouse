import mysql.connector
from collections import defaultdict
import data_warehouse
import pandas as pd
import statistics

def get_purchase_historys(db_cursor, table_name):
    print("Datenabruf gestartet...")
    data: pd.DataFrame = data_warehouse.fetch_table_data(db_cursor, table_name)
    if data is None or data.empty:
        print("Keine Daten gefunden.")
        return []

    grouped = data.groupby("order_id")["product_id"].apply(lambda x: x.astype(str).tolist())
    return grouped.tolist()

def support_details(product_support, num_to_display=3):
    result = ""
    total_products_count = len(product_support)
    support_values = list(product_support.values())
    max_support = max(support_values)
    min_support = min(support_values)
    median_support = statistics.median(support_values)

    max_products = [p for p, s in product_support.items() if s == max_support]
    min_products = [p for p, s in product_support.items() if s == min_support]
    median_products = [p for p, s in product_support.items() if s == median_support]

    result += f"Insgesamt gibt es {total_products_count} Produkte.\n"
    result += f"Median Support: {median_support:.6f}\n"
    result += f"Höchster Support: {max_support:.6f} ({len(max_products)} Produkte)\n"
    result += f"Niedrigster Support: {min_support:.6f} ({len(min_products)} Produkte)\n"

    result += "\nBeispiele für Produkte mit Median Support:\n"
    for p in median_products[:num_to_display]:
        result += f"- {p} (Support: {median_support:.6f})\n"

    result += "\nBeispiele für Produkte mit höchstem Support:\n"
    for p in max_products[:num_to_display]:
        result += f"- {p} (Support: {max_support:.6f})\n"

    result += "\nBeispiele für Produkte mit niedrigstem Support:\n"
    for p in min_products[:num_to_display]:
        result += f"- {p} (Support: {min_support:.6f})\n"

    return result

def confidence_lift_details(confidence_lift_data, num_to_display=3):
    result = ""

    confidence_values = [v["confidence"] for v in confidence_lift_data.values()]
    lift_values = [v["lift"] for v in confidence_lift_data.values()]

    median_conf = statistics.median(confidence_values)
    median_lift = statistics.median(lift_values)
    max_conf = max(confidence_values)
    min_conf = min(confidence_values)
    max_lift = max(lift_values)
    min_lift = min(lift_values)

    max_conf_pairs = [p for p, v in confidence_lift_data.items() if v["confidence"] == max_conf]
    min_conf_pairs = [p for p, v in confidence_lift_data.items() if v["confidence"] == min_conf]
    median_conf_pairs = [p for p, v in confidence_lift_data.items() if v["confidence"] == median_conf]

    max_lift_pairs = [p for p, v in confidence_lift_data.items() if v["lift"] == max_lift]
    min_lift_pairs = [p for p, v in confidence_lift_data.items() if v["lift"] == min_lift]
    median_lift_pairs = [p for p, v in confidence_lift_data.items() if v["lift"] == median_lift]

    result += f"\n--- Confidence Analyse ---\n"
    result += f"Median Confidence: {median_conf:.6f}\n"
    result += f"Höchste Confidence: {max_conf:.6f} ({len(max_conf_pairs)} Paare)\n"
    result += f"Niedrigste Confidence: {min_conf:.6f} ({len(min_conf_pairs)} Paare)\n"

    result += "\nBeispiele für Paare mit Median Confidence:\n"
    for p in median_conf_pairs[:num_to_display]:
        result += f"- {p} (Confidence: {median_conf:.6f})\n"

    result += "\nBeispiele für Paare mit höchster Confidence:\n"
    for p in max_conf_pairs[:num_to_display]:
        result += f"- {p} (Confidence: {max_conf:.6f})\n"

    result += "\nBeispiele für Paare mit niedrigster Confidence:\n"
    for p in min_conf_pairs[:num_to_display]:
        result += f"- {p} (Confidence: {min_conf:.6f})\n"

    result += f"\n--- Lift Analyse ---\n"
    result += f"Median Lift: {median_lift:.6f}\n"
    result += f"Höchster Lift: {max_lift:.6f} ({len(max_lift_pairs)} Paare)\n"
    result += f"Niedrigster Lift: {min_lift:.6f} ({len(min_lift_pairs)} Paare)\n"

    result += "\nBeispiele für Paare mit Median Lift:\n"
    for p in median_lift_pairs[:num_to_display]:
        result += f"- {p} (Lift: {median_lift:.6f})\n"

    result += "\nBeispiele für Paare mit höchstem Lift:\n"
    for p in max_lift_pairs[:num_to_display]:
        result += f"- {p} (Lift: {max_lift:.6f})\n"

    result += "\nBeispiele für Paare mit niedrigstem Lift:\n"
    for p in min_lift_pairs[:num_to_display]:
        result += f"- {p} (Lift: {min_lift:.6f})\n"

    return result

def calculate_support_confidence_lift(sales_data):
    product_support = defaultdict(int)
    total_transactions = len(sales_data)

    for sale in sales_data:
        for product_id in sale:
            product_support[product_id] += 1

    product_support = {k: v / total_transactions for k, v in product_support.items()}

    product_pair_support = defaultdict(int)
    for sale in sales_data:
        for i in range(len(sale)):
            for j in range(i + 1, len(sale)):
                pair = tuple(sorted([sale[i], sale[j]]))
                product_pair_support[pair] += 1

    confidence_lift = {}
    for pair, pair_count in product_pair_support.items():
        a, b = pair
        support_a = product_support[a]
        support_b = product_support[b]
        confidence = pair_count / (support_a * total_transactions)
        lift = confidence / support_b
        confidence_lift[pair] = {"confidence": confidence, "lift": lift}

    return product_support, confidence_lift

def main():
    with data_warehouse.connect_to_db() as db:
        with db.cursor() as cursor:
            cursor.execute("USE retailsalesdw")
            sales_data = get_purchase_historys(cursor, "order_products__train")
            if not sales_data:
                return
            product_support, confidence_lift = calculate_support_confidence_lift(sales_data)
            print(support_details(product_support))
            print(confidence_lift_details(confidence_lift))
            cursor.fetchall()

if __name__ == "__main__":
    main()
