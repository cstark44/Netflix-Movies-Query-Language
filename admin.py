import json
import sys
import firebase_admin
from firebase_admin import db, credentials, firestore

# Make sure the user passed in a file
if len(sys.argv) != 2:
    print("Expected 2 arguments: python admin.py <data.json>")

else:
    try:
        # Connect to firebase
        cred = credentials.Certificate('credentials.json')
        app = firebase_admin.initialize_app(cred)
        db = firestore.client()
        movies = db.collection("movies")

        # Delete all the documents in the collection
        docs = db.collection("movies").list_documents()
        for doc in docs:
            doc.delete()

        # Add movies from json file
        with open(sys.argv[1]) as data_file:
            data = json.load(data_file)
            for entry in data:
                # ----------------------------------------------------------------- 
                # Delete entries where the value is "N/A"
                to_delete = []
                for field in entry:
                    if entry[field] == "N/A":
                        to_delete.append(field)
                for field in to_delete:
                    entry.pop(field)
                # -----------------------------------------------------------------

                db.collection("movies").add(entry)
    except:
        print("Error: File not found")