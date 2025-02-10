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
with genre *genre*
released in *year*
)
'''

def get_records(column: list[str], operation: list[str], condition: list[str], return_col: list[str]):
    print("_______ GETTING RECORDS _______")
    print(column, operation, condition, return_cols)
    to_db_col_name = {
        'movie_title' : 'TITLE',
        'director' : 'DIRECTOR',
        'genre' : 'GENRE',
        'release_date' : 'YEAR',
        'rating' : 'RATING',
        'runtime' : 'RUNTIME',
        'cast' : 'CAST'
    }
    query_to_do = movies
    # For each query joined with an 'and', add another where clause
    for i in range(len(column)):
        query_to_do = query_to_do.where(filter=FieldFilter(to_db_col_name[column[i]], operation[i], condition[i]))
        results = query_to_do.get()

        # Multiple conditions
        if (i > 0):
            uuids = []
            # Go through results and save each UUID
            for mov in results:
                uuids.append(mov._data["UUID"])
            
            query_to_do = movies.where(filter=FieldFilter("UUID", "in", uuids))

    if len(results) == 0:
        print("No movies found")

    for mov in results:
        if return_col == []:
            print(mov._data)
        else:
            for col in return_col:
                print(mov._data[to_db_col_name[col]])

while True:
    phrase = input("Query: ")

    #keywords involing title input
    info = CaselessKeyword("info about")
    director_of = CaselessKeyword("director of")
    cast = CaselessKeyword("cast of")
    duration_of = CaselessKeyword("duration of")
    rating_of = CaselessKeyword("rating of")
    release_date = CaselessKeyword("release date of")
    movie_called = CaselessKeyword("movie called")
    genre_of = CaselessKeyword("genre of")

    #keyword for release date
    released_in = CaselessKeyword("released in")

    #keywords for specific attributes
    starring = CaselessKeyword("starring")
    directed_by = CaselessKeyword("directed by")
    genre_with = CaselessKeyword("with genre")


    keywords = (info | director_of | cast | duration_of | rating_of | release_date | movie_called | genre_of 
                | released_in | starring | directed_by | genre_with)

    split_queries = phrase.split("and")

    parse_input = keywords + OneOrMore(Word(alphanums))


    columns = []
    operators = []
    conditions = []
    return_cols = []
    return_flag = 0

    # Iterate through each query that was joined with an 'and'
    for query in split_queries:
        # Parse
        try:
            cur_query = parse_input.parseString(query)
        except ParseException as e:
            print(f"Error: Unable to parse query '{query}'. Type 'Help' for more options.")
        keyword = cur_query[0]

        # Make parsed condition into a str
        cond = ""
        for i in range(1, len(cur_query)):
            cond += cur_query[i]
            if i != len(cur_query) - 1:
                cond += " "

        # Match keyword and set proper conditions to return
        match keyword:
            # Inputs involving movie titles
            case "info about" | "director of" | "cast of" | "duration of" | "rating of" | "release date of" | "movie called" | "genre of":
                # Filter where movie_title == condition
                columns.append("movie_title")
                operators.append("==")
                try:
                    conditions.append(str(cond))
                except ValueError:
                    print("Value must be string. Input a movie title.")
                    return_flag+1

                # Add column to return depending on the query
                match keyword:
                    case "director of":
                        return_cols.append("director")
                    case "cast of":
                        return_cols.append("cast")
                    case "duration of":
                        return_cols.append("runtime")
                    case "rating of":
                        return_cols.append("rating")
                    case "release date of":
                        return_cols.append("release_date")
                    case "genre of":
                        return_cols.append("genre")


            # Inputs involving release dates
            case "released in":
                columns.append("release_date")
                operators.append("==")
                try:
                    conditions.append(int(cond))
                except ValueError:
                    print("Value must be an integer. Input a year.")
                    return_flag+1
            # Inputs involving specific attributes
            case "starring":
                columns.append("cast")
                operators.append("array_contains")
                try:
                    conditions.append(str(cond))
                except ValueError:
                    print("Value must be a name. Input an actor/actress.")
                    return_flag+1
            case "directed by":
                columns.append("director")
                operators.append("==")
                try:
                    conditions.append(str(cond))
                except ValueError:
                    print("Value must be a name. Input a director's name.")
                    return_flag+1
            case "with genre":
                columns.append("genre")
                operators.append("array_contains")
                try:
                    conditions.append(str(cond))
                except ValueError:
                    print("Value must be a word. Input a genre.")
                    return_flag+1


    get_records(columns, operators, conditions, return_cols)



