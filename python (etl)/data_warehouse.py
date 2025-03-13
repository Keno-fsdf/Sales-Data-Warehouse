import mysql.connector

# Datenbankverbindung herstellen
db = mysql.connector.connect(
    host="localhost",
    user="root",  # Passe den Benutzernamen an
    password="Apfelkuchen",  # Passe das Passwort an
)

db_cursor = db.cursor()

# Sicherstellen, dass alle offenen Ergebnisse geleert werden
try:
    db_cursor.fetchall()  # Alle offenen Ergebnisse leeren
except:
    pass  # Falls keine offenen Ergebnisse da sind, einfach weitermachen

# Erstelle Datenbank und Tabellen mit Transaktionen
def create_data_warehouse():
    try:
        # Transaktion starten
        db_cursor.execute("BEGIN")

        # Datenbank erstellen, falls sie nicht existiert
        db_cursor.execute("CREATE DATABASE IF NOT EXISTS RetailSalesDW")
        db_cursor.execute("USE RetailSalesDW")

        # Tabellen für Sales-Daten erstellen
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
                preis DECIMAL(10, 2)
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
                gesamtbetrag DECIMAL(10, 2),
                FOREIGN KEY (kunden_id) REFERENCES Kunden(kunden_id) ON DELETE CASCADE,
                FOREIGN KEY (produkt_id) REFERENCES Produkte(produkt_id) ON DELETE CASCADE,
                FOREIGN KEY (filial_id) REFERENCES Filialen(filial_id) ON DELETE CASCADE,
                FOREIGN KEY (zeitraum_id) REFERENCES Zeitraeume(zeitraum_id) ON DELETE CASCADE
            )
            """
        ]

        # Alle Tabellen erstellen
        for query in table_queries:
            db_cursor.execute(query)

        # Transaktion erfolgreich abschließen
        db.commit()
        print("Datenbank und Tabellen wurden erfolgreich erstellt.")

    except Exception as e:
        # Falls ein Fehler auftritt, die Transaktion zurückrollen
        db.rollback()
        print(f"Fehler beim Erstellen der Datenbank und Tabellen: {e}")

# Methode aufrufen
create_data_warehouse()

# Verbindung schließen
db_cursor.close()
db.close()
