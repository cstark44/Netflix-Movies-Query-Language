from pyparsing import *
import json
import firebase_admin
from firebase_admin import db, credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter

cred = credentials.Certificate('credentials.json')
app = firebase_admin.initialize_app(cred)
db = firestore.client()
movies = db.collection("movies")

'''
Cols:
    Title
    Director
    Cast
    Rating
    Genre
    Runtime
    Year

info about *tile*
directer of *title*
cast of *title*
rating of *title*
genre of *title*
runtime of *title*
release data of *title*

directed by *director*
starring *actor*
rated above *num*
rated below *num*
with genre *genre*
longer than *time* 
shorter than *time*
released before *year*
released after  *year*
released in *year*
)
'''

def get_records(column: list[str], operation: list[str], condition: list[str], list_all: list[str]):
    to_db_col_name = {
        'movie_title' : 'title',
        'director' : 'director',
        'genre' : 'genre',
        'released' : 'year'
    }
    query_to_do = movies
    for i in range(len(column)):
        query_to_do = query_to_do.where(filter=FieldFilter(to_db_col_name[column[i]], operation[i], condition[i]))

    results = query_to_do.get()

    for mov in results:
        if list_all == []:
            print(mov._data)
        else:
            for col in list_all:
                print(mov._data[col])

phrase = "genre of Dinosaur Planet and release date of Dinosaur Planet" # and directed by Greta Gerwig"

shorter_than = CaselessKeyword("shorter than")
longer_than = CaselessKeyword("longer than")
released_before = CaselessKeyword("released before")
starring = CaselessKeyword("starring")
movie_called = CaselessKeyword("movie called")
directed_by = CaselessKeyword("directed by")
genre_of = CaselessKeyword("genre of")
genre_with = CaselessKeyword("with genre")
release_date = CaselessKeyword("release date of")


keywords = (shorter_than | longer_than | released_before | starring | movie_called | directed_by | genre_of | genre_with | release_date)

split_queries = phrase.split("and")

parse_input = keywords + OneOrMore(Word(alphanums))


columns = []
operators = []
conditions = []
list_all = []

print(phrase)
for query in split_queries:
    temp = parse_input.parseString(query)
    
    
    match temp[0]:
        case "starring":
            columns.append("cast")
            operators.append("==")
            str_name = ""
            for i in range(1, len(temp)):
                str_name += temp[i]
                if i != len(temp) - 1:
                    str_name += " "
            conditions.append(str_name)

        case "movie called":
            columns.append("movie_title")
            operators.append("==")
            str_name = ""
            for i in range(1, len(temp)):
                str_name += temp[i]
                if i != len(temp) - 1:
                    str_name += " "
            conditions.append(str_name)

        case "directed by":
            columns.append("director")
            operators.append("==")
            str_name = ""
            for i in range(1, len(temp)):
                str_name += temp[i]
                if i != len(temp) - 1:
                    str_name += " "
            conditions.append(str_name)

        case "with genre":
            columns.append("genre")
            operators.append("array_contains")
            str_name = ""
            for i in range(1, len(temp)):
                str_name += temp[i]
                if i != len(temp) - 1:
                    str_name += " "
            conditions.append(str_name)

        case "genre of":
            columns.append("movie_title")
            operators.append("==")
            str_name = ""
            for i in range(1, len(temp)):
                str_name += temp[i]
                if i != len(temp) - 1:
                    str_name += " "
            conditions.append(str_name)
            list_all.append("genre")

        case "release date of":
            columns.append("movie_title")
            operators.append("==")
            str_name = ""
            for i in range(1, len(temp)):
                str_name += temp[i]
                if i != len(temp) - 1:
                    str_name += " "
            conditions.append(str_name)
            list_all.append("year")


# print(columns)
# print(operators)
# print(conditions)


get_records(columns, operators, conditions, list_all)

    

# parsed = shorter_than.parseString(phrase)

# keyword_given = parsed[0]


