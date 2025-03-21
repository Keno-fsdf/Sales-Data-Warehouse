import mysql.connector
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os



#KURZE ANMERKUNG AN MICH SELBER: wenn ich das irgendwann auf meinem laptop nutzen will, dann muss ich auc eine .env datei erstellen mit dem gleichen inhalt und eben alles was in gitignore drin ist beachten.  


# Lädt die Umgebungsvariablen aus der .env-Datei
load_dotenv()
# Verbindungsdetails aus den Umgebungsvariablen
db_host = os.getenv("DB_HOST")
print(db_host)
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")


#ich weiß es ist bad practices wie man hier die daten sieht. Aber weil es nichts sensibles ist und nur eine schwachstelle aber keine verwundbarkeit ist, ist es kurzfristig akzeptabel, bis ich das mit problem mit meinen env datein herausgefunden habe.
#Methode um sich mit der Datenbank zu verbinden
def connect_to_db():
    return mysql.connector.connect(
        host= "localhost",
        user="root",
        password="Apfelkuchen"
    )


"""
# Methode um sich mit der Datenbank zu verbinden
def connect_to_db():
    return mysql.connector.connect(
        
        host=db_host,
        user=db_user,
        password=db_password
    )
"""




#DocStrings kann ich noch hhinzufügen wenn ich das will mal

#ANMERKUNG zu den SQL Statement. Da es sich lediglich um ein lokal genutzes Data-Warehouse handelt, sind SQL-Injektion Angriffe nicht wirklich relevant und somit hab ich auch direkte SQL-Statements benutzt aus Komfort

#Vielleicht noch "logging" nutzen anstatt simple print statements. Maybe irgendwann noch umändern, aber zunächst passt das so.

# Funktion, um automatisch die SQL-Tabelle basierend auf der CSV-Struktur zu erstellen


# Funktion zur Bestimmung des SQL-Datentyps basierend auf dem Pandas-Datentyp
def get_sql_datatype(dtype):
    if pd.api.types.is_integer_dtype(dtype):    # Diese Zeile prüft, ob der Datentyp der Spalte ein ganzzahliger (integer) Typ ist, z.B. int64, int32 oder ähnliche Pandas-Typen.
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):   #Diese Zeile prüft, ob der Datentyp der Spalte ein Fließkomma-Typ ist, z.B. float64, float32 usw.

        return "FLOAT"
    elif pd.api.types.is_datetime64_any_dtype(dtype):  #Diese Zeile prüft, ob der Datentyp der Spalte ein Datums- oder Zeitwert ist. Pandas unterstützt verschiedene Datums- und Zeittypen wie datetime64[ns], datetime64[ns, tz], timedelta64[ns] usw.
        return "DATETIME"
    else:
        return "VARCHAR(255)" #Wenn keiner der obigen Tests zutrifft, geht der Code davon aus, dass der Datentyp der Spalte ein Textfeld ist.



def create_data_warehouse(db, db_cursor, database_name):
    """
    Erstellt eine MySQL-Datenbank innerhalb einer Transaktion.
    """
    try:
        db.start_transaction()  # Transaktion starten
        db_cursor.execute(f"SHOW DATABASES LIKE '{database_name}'")
        database_exists = db_cursor.fetchone()      #fetchone() holt genau eine Zeile aus den Ergebnissen des vorherigen execute()-Befehls.


        if database_exists:    #Anmerkung nur an mich: Boah ich finde python teilweise so komisch, hier hat man ja nicht mal ein boolean und das geht trotzdem
            print(f"ℹDie Datenbank '{database_name}' existiert bereits.")
            db.rollback()  # Transaktion abbrechen, weil nichts geändert wurde
        else:
            db_cursor.execute(f"CREATE DATABASE {database_name}")
            db.commit()  # Transaktion erfolgreich abschließen
            print(f"Die Datenbank '{database_name}' wurde erfolgreich erstellt.")

    except Exception as e:
        db.rollback()  # Falls ein Fehler auftritt, Änderungen zurücksetzen
        print(f"Fehler beim Erstellen der Datenbank '{database_name}': {e}")




# Funktion, um automatisch eine Tabelle aus einer CSV-Datei zu erstellen
def create_table_from_csv(db, csv_file,  db_cursor):

    try: 
        db.start_transaction()
        # CSV-Datei einlesen
        df = pd.read_csv(csv_file)


        #Einlesen der Tabellen Bezeichnung durch die Dateibezeichnung.

        table_name = os.path.splitext(os.path.basename(csv_file))[0] #gibt ein tupel zurück mit dateibezeichnung und dateiendung und somit nehmen ich dann nur den ersten teil mit "0"

        # SQL-Statement für die Tabellenerstellung
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        
        # Spaltennamen und Datentypen dynamisch bestimmen
        for column in df.columns:
            column_type = get_sql_datatype(df[column])  # Bestimme den SQL-Datentyp
            create_table_sql += f"{column} {column_type}, "

        # Entferne das letzte Komma und schließe die Klammer
        create_table_sql = create_table_sql.rstrip(", ") + ")"

        # SQL-Statement ausführen
        db_cursor.execute(create_table_sql)
        db.commit()
        print(f"Tabelle '{table_name}' erfolgreich erstellt.")
    except Exception as e:
        db.rollback()  # Falls ein Fehler auftritt, Änderungen zurücksetzen
        print("Fehler bei der Erstellung der Tabellen aufgetreten")
        print(e)





#Funktion zum Importieren von CSV-Dateien in die Datenbank
#Problem mit NaN und Problem -Bob's Mash wird zu 'Bob's Mash', was ein Syntaxfehler in SQL ist- behoben

def import_csv_to_db(db, csv_file, db_cursor):
    try:
        db.start_transaction()  # Transaktion starten

        # Tabellenname aus Dateinamen extrahieren
        table_name = os.path.splitext(os.path.basename(csv_file))[0]

        df = pd.read_csv(csv_file)   #Ladt die daten in DataFrames  -->Dadurch enstehen "NaN, wenn irgendwo kein wert vorhanden ist"
        df = df.where(pd.notnull(df), None) #Ersetzt alle NaN-Werte durch None, damit MySQL sie als NULL erkennt



        #FIX für: "Python type numpy.int64 cannot be converted"

        # Bestimme die SQL-Datentypen und konvertiere die Spalten entsprechend
        column_types = {col: get_sql_datatype(df[col].dtype) for col in df.columns}


        #ALSO ES SCHEINT ZU FUNKTIONIEREN, aber ich hab irgendwie kein plan wieso lol
        for col, sql_dtype in column_types.items():
            if sql_dtype == "INT":
                # Ändere den Datentyp zu int64 (statt int32)
                df[col] = df[col].astype('Int64', errors='ignore')  # Sicherstellung von int64
            elif sql_dtype == "FLOAT":
                # Ändere den Datentyp zu float64
                df[col] = df[col].astype('float64', errors='ignore')  # Sicherstellung von float64
            elif sql_dtype == "DATETIME":
                # Wandle die Spalte in Datetime um (im Fehlerfall wird "coerce" verwendet, d.h. ungültige Werte werden zu NaT)
                df[col] = pd.to_datetime(df[col], errors="coerce")
        
        


        # SQL-Statement mit Platzhaltern
        columns = ", ".join(f"`{col}`" for col in df.columns)  # "id", "name", "price" -->Wird zu `id`, `name`, `price`. -->Das verhindert gewisse probleme
        placeholders = ", ".join(["%s"] * len(df.columns))  # Platzhalter für Werte
        insert_sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"  #baut das sql statement zusammen

        # Daten einfügen
        for _, row in df.iterrows():  # Iteriere über jede Zeile im DataFrame
            values = tuple(None if pd.isna(val) else val for val in row.values)  # NaN -> NULL
            # Führe das SQL-Insert-Statement aus und füge die Werte in die Datenbank ein
            db_cursor.execute(insert_sql, values)  

        # Transaktion abschließen
        db.commit()
        print(f"Daten in die Tabelle '{table_name}' erfolgreich eingefügt.")
    except Exception as e:
        db.rollback()
        print(f"Fehler beim Importieren der Tabelle '{table_name}': {e}")






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
def fetch_table_data(db_cursor, table_name):
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


#Bei gelegenheit noch das als funktion mit transaktion machen.
#noch so umändern dass keine duplikate erstlelt werden an tabellen

#Dann noch ein autodrop von der datenbank machen wenn man auf ja drückt

def finaleErstellungWarehouse():

    confirm_string = input("Bestätige, dass du das Data Warehouse erstellen willst (Ja/Nein): ")

    if confirm_string.lower() == "ja":
        if __name__ == "__main__":   
            with connect_to_db() as db:
                with db.cursor() as db_cursor:
                    set_isolation_level(db, db_cursor, "SERIALIZABLE")  #Aufrufen der methoden um den isolationsgrad zu setzten udn danach das  data warehouse zu erstellen.
                    try: 
                        db.start_transaction()

                        db_cursor.execute("Drop database retailsalesdw")
                        create_data_warehouse(db, db_cursor, "retailsalesdw")
                        db_cursor.execute("USE retailsalesdw")

                        # Liste der CSV-Dateien, die du importieren möchtest
                        csv_files = [
                        r"C:\Users\Keno\Desktop\Sales-Data-Warehouse\aisles.csv",
                        r"C:\Users\Keno\Desktop\Sales-Data-Warehouse\departments.csv",
                        r"C:\Users\Keno\Desktop\Sales-Data-Warehouse\order_products__train.csv",
                        r"C:\Users\Keno\Desktop\Sales-Data-Warehouse\order_products__prior.csv",
                        r"C:\Users\Keno\Desktop\Sales-Data-Warehouse\orders.csv",
                        r"C:\Users\Keno\Desktop\Sales-Data-Warehouse\products.csv"
                            ]    
                        # Für jede CSV-Datei erstellen und importieren
                        for csv_file in csv_files:
                            
                                # Zuerst die Tabelle aus der CSV-Datei erstellen
                                create_table_from_csv(db, csv_file, db_cursor)

                                # Dann die Daten in die Tabelle importieren
                                import_csv_to_db(db, csv_file, db_cursor)

                            
                        
                         
                        db.commit()
                        print("Data Warehouse erfolgreich erstellt")
                    except Exception as e:
                        print(e)
                        db.rollback()
                        print(f"Fehler beim Erstellen des Data Warehouses: {e}")
    else:
        print("Der Vorgang wurde abgebrochen.")

if __name__ == "__main__":       #das is sehr wichtig, weil sonst ein import dieses skriptes bei einem anderen skript automatisch in python dieses auslöst, sofern der code nicht in funktionen ist.
    finaleErstellungWarehouse()

