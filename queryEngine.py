# Query Engine File
from pyparsing import *
import json
import firebase_admin
from firebase_admin import db, credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter

# Connect to firebase
cred = credentials.Certificate('credentials.json')
app = firebase_admin.initialize_app(cred)
db = firestore.client()
movies = db.collection("movies")

def help():

    while True:
        print("--------------------------------------------------------------------------------")
        print("                                  HELP MENU                                     ")
        print("--------------------------------------------------------------------------------")
        print("Use this tool to discover the perfect Netflix movie for your next movie night!\n"
              "Simply combine any of the available keywords, and the database will find a match\n"
              "to help you choose what to watch.")
        print("Example Inputs:")
        print("- movies released in 2012.")
        print("- movies directed by Tim Burton and with\n"
              "genre comedy")
        print("The keywords that can be used when typing in an input include:")
        print("- info about")
        print("- director of")
        print("- cast of")
        print("- duration of")
        print("- rating of")
        print("- release date of")
        print("- movie called")
        print("- genre of")
        print("- released in")
        print("- starring")
        print("- directed by")
        print("- with genre")
        print("Type EXIT to exit the Help menu")

        user_input = input("Enter 'EXIT' to leave the help menu: ")

        if user_input == "EXIT":
            print("Returning to query\n")
            break

# function: user_query
# - asks user to enter a query
# - stores input as 'query'
def user_query():
    while True:
        print("Enter 'Help' to get an explanation of this program")
        query = input("Enter query: ")

        if query == "Help":
            help()
        else:
            return query

# function: parse_input
# - takes in user_input
# - parses user_input to meet criteria
# - returns columnQuerying, operator, criteria, listAllFlag
def parse_input(user_input):
    # keywords involving title input
    info = CaselessKeyword("info about")
    director_of = CaselessKeyword("director of")
    cast = CaselessKeyword("cast of")
    duration_of = CaselessKeyword("duration of")
    rating_of = CaselessKeyword("rating of")
    release_date = CaselessKeyword("release date of")
    movie_called = CaselessKeyword("movie called")
    genre_of = CaselessKeyword("genre of")

    # keyword for release date
    released_in = CaselessKeyword("released in")

    # keywords for specific attributes
    starring = CaselessKeyword("starring")
    directed_by = CaselessKeyword("directed by")
    genre_with = CaselessKeyword("with genre")


    keywords = (info | director_of | cast | duration_of | rating_of | release_date | movie_called | genre_of 
                | released_in | starring | directed_by | genre_with)

    split_queries = user_input.split("and")

    parse_input = keywords + OneOrMore(Word(alphanums))

    # Set up return vars
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
            print(f"Error: Unable to parse query '{query}'. {str(e)}")
            return [], [], [], [], 1

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
    
    # Return
    return columns, operators, conditions, return_cols, return_flag

# function: get_records
# - takes in columnQuerying, operator, criteria, listAllFlag
# - returns string of specific record
def get_records(columnQuerying, operator, criteria, return_cols):
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
    for i in range(len(columnQuerying)):
        query_to_do = query_to_do.where(filter=FieldFilter(to_db_col_name[columnQuerying[i]], operator[i], criteria[i]))
        results = query_to_do.get()

        # Multiple conditions
        if (i > 0):
            uuids = []
            # Go through results and save each UUID
            for mov in results:
                uuids.append(mov._data["UUID"])
            
            query_to_do = movies.where(filter=FieldFilter("UUID", "in", uuids))
    
    # Return results
    return results

# function: print_results
def print_results(results):
    print("\n Movie Results \n" + "-" * 50)
    # printing for movies found
    # ex. if the director can't be found it will print N/A
    for mov in results:
        data = mov.to_dict()
        print(f" Title         : {data.get('TITLE', 'N/A')}")
        print(f" Director      : {data.get('DIRECTOR', 'N/A')}")
        print(f" Genre         : {data.get('GENRE', 'N/A')}")
        print(f" Release Date  : {data.get('YEAR', 'N/A')}")
        print(f" Rating        : {data.get('RATING', 'N/A')}")
        print(f" Runtime       : {data.get('RUNTIME', 'N/A')}")
        print(f" Cast          : {data.get('CAST', 'N/A')}")

        if i < len(results) - 1:
            print("\n")

# this might not be needed/ not totally sure if firestore does this
# function: run_query_engine
# - while true loop
# - calls user_query, parse_input, get_records
# - prints results
#def run_query_engine():

def main():
    while True:
        # Get user query
        query = user_query().rstrip()

        if query == "quit":
            print("Quitting query engine")
            break

        # Parse
        columnQuerying, operator, criteria, return_cols, return_flag = parse_input(query)

        # Do the query
        if (return_flag == 0):
            results = get_records(columnQuerying, operator, criteria, return_cols)

            # Temp print
            print_results(results)

main()

