import json
import firebase_admin
from firebase_admin import db, credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter

cred = credentials.Certificate('credentials.json')
app = firebase_admin.initialize_app(cred)
db = firestore.client()
movies = db.collection("movies")

# with open('movies.json') as data_file:
#     data = json.load(data_file)
#     for entry in data:
#         db.collection("movies").add(data[entry])

# print(movies.where(filter=FieldFilter("title", "==", "Little Women")).where(filter=FieldFilter("director", "==", "Greta Gerwig")).get()[0]._data)

def do_query(column: list[str], operation: list[str], condition: list[str], list_all: bool):
    to_db_col_name = {
        'movie_title' : 'title',
        'director' : 'director'
    }
    query_to_do = movies
    for i in range(len(column)):
        query_to_do = query_to_do.where(filter=FieldFilter(to_db_col_name[column[i]], operation[i], condition[i]))

    results = query_to_do.get()

    for mov in results:
        print(mov._data)

col = ['movie_title', 'director']
op = ['==', '==']
cond = ['Little Women', 'Greta Gerwig']

do_query(col, op, cond, True)




# db.collection("movies").add(data)