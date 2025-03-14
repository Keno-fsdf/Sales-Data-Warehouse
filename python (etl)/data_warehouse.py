import mysql.connector
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine


#Methode um sich mit der Datenbank zu verbinden
def connect_to_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Apfelkuchen"
    )



#ANMERKUNG zu den SQL Statement. Da es sich lediglich um ein lokal genutzes Data-Warehouse handelt, sind SQL-Injektion Angriffe nicht wirklich relevant und somit hab ich auch direkte SQL-Statements benutzt aus Komfort

#Vielleicht noch "logging" nutzen anstatt simple print statements. Maybe irgendwann noch umändern, aber zunächst passt das so.

# Funktion, um automatisch die SQL-Tabelle basierend auf der CSV-Struktur zu erstellen





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
            #create_data_warehouse(db, db_cursor)
