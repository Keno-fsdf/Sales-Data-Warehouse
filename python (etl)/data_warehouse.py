import mysql.connector



#Methode um sich mit der Datenbank zu verbinden
def connect_to_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Apfelkuchen",
    )



#ANMERKUNG zu den SQL Statement. Da es sich lediglich um ein lokal genutzes Data-Warehouse handelt, sind SQL-Injektion Angriffe nicht wirklich relevant und somit hab ich auch direkte SQL-Statements benutzt aus Komfort

#Vielleicht noch "logging" nutzen anstatt simple print statements. Maybe irgendwann noch umändern, aber zunächst passt das so.

#Erstellung der Datawarehoused (Datenbank) falls sie nicht existiert
def create_data_warehouse(db, db_cursor):

    try:
        db_cursor.execute("BEGIN")  #Nutzung von Transaktionen für Rollbacks bei Fehlern

        # Datenbank erstellen und nutzen
        db_cursor.execute("CREATE DATABASE IF NOT EXISTS RetailSalesDW")
        db_cursor.execute("USE RetailSalesDW")

        # Tabellen erstellen
        table_queries = [
            """
            CREATE TABLE IF NOT EXISTS Kunden (
                kunden_id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255),
                geburtsdatum DATE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS Produkte (
                produkt_id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                kategorie VARCHAR(255),
                preis DECIMAL(10,2)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS Filialen (
                filial_id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                standort VARCHAR(255)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS Zeitraeume (
                zeitraum_id INT AUTO_INCREMENT PRIMARY KEY,
                monat INT NOT NULL,
                jahr INT NOT NULL,
                quartal INT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS Sales (
                sales_id INT AUTO_INCREMENT PRIMARY KEY,
                kunden_id INT,
                produkt_id INT,
                filial_id INT,
                zeitraum_id INT,
                anzahl INT,
                gesamtbetrag DECIMAL(10,2),
                FOREIGN KEY (kunden_id) REFERENCES Kunden(kunden_id) ON DELETE CASCADE,
                FOREIGN KEY (produkt_id) REFERENCES Produkte(produkt_id) ON DELETE CASCADE,
                FOREIGN KEY (filial_id) REFERENCES Filialen(filial_id) ON DELETE CASCADE,
                FOREIGN KEY (zeitraum_id) REFERENCES Zeitraeume(zeitraum_id) ON DELETE CASCADE
            )
            """
        ]

        for query in table_queries:
            db_cursor.execute(query)

        db.commit()
        print("Datenbank und Tabellen wurden erfolgreich erstellt.")

    except Exception as e:
        db.rollback()
        print(f"Fehler beim Erstellen der Datenbank und Tabellen: {e}")


#Methode zum setzten und prüfen des Isolationsgrad.
def set_isolation_level(db, db_cursor, isolation_level="REPEATABLE READ"):
    try:
        db_cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")

        # Isolationsgrad überprüfen
        db_cursor.execute("SELECT @@transaction_isolation")
        current_isolation_level = db_cursor.fetchone()[0]

        print(f"Der Isolationsgrad wurde auf '{current_isolation_level}' gesetzt.")

    except Exception as e:
        print(f"Fehler beim Setzen des Isolationsgrads: {e}")

def exists(db_cursor, query, params):
    """Prüft, ob ein Eintrag bereits existiert."""
    db_cursor.execute(query, params)
    return db_cursor.fetchone() is not None




# Funktion zum Abrufen und Anzeigen der Tabelleninhalte mit Attributen
#WICHTIG: Ich nutzte jetzt hier keine Transakion, da es nur ein Select statement ist und ich das risiko von dirty reads jetzt hier eingehe!!!
def fetch_table_data(table_name):
    query = f"SELECT * FROM {table_name}"
    db_cursor.execute(query)

    # Spaltennamen abrufen
    column_names = [i[0] for i in db_cursor.description]

    # Daten abrufen
    rows = db_cursor.fetchall()

    # Tabellenüberschrift ausgeben
    print(f"\nInhalt der Tabelle '{table_name}':")
    print(" | ".join(column_names))  # Spaltennamen anzeigen
    print("-" * 50)

    # Datenzeilen ausgeben
    for row in rows:
        print(" | ".join(str(x) for x in row))








# Hauptprogramm
"""
if __name__ == "__main__": sorgt dafür, dass dieses Skript nur dann ausgeführt wird,  
wenn es direkt gestartet wird. Falls das Skript importiert wird,  
wird dieser Codeblock nicht ausgeführt.
"""
"""
Die with-Anweisung in Python funktioniert im Wesentlichen genauso wie das try-with-resources Statement in Java.
Beide Konzepte dienen dazu, Ressourcen sicher zu verwalten und nach der Nutzung automatisch freizugeben, was besonders wichtig ist, um Ressourcenlecks zu verhindern.
Also immer schön das benutzen damit ich nicht irgendwelche offenen Ressourcen habe.

Also anscheinend definiert das ein Kontextmanager mit den Methoden "__enter__()" und "__exit__()"

"""
if __name__ == "__main__":   
    with connect_to_db() as db:
        with db.cursor() as db_cursor:
            set_isolation_level(db, db_cursor, "SERIALIZABLE")  #Aufrufen der methoden um den isolationsgrad zu setzten udn danach das  data warehouse zu erstellen.
            create_data_warehouse(db, db_cursor)
