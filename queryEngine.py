# Query Engine File

# Load in JSON File

# function: user_query
# - asks user to enter a query
# - stores input as 'query'
def user_query():
    query = input("Enter query: ")
    return query

# function: parse_input
# - takes in user_input
# - parses user_input to meet criteria
# - returns columnQuerying, operator, criteria, listAllFlag
#def parse_input(user_input):

# function: get_records
# - takes in columnQuerying, operator, criteria, listAllFlag
# - returns string of specific record
#def get_records(columnQuerying, operator, criteria, listAllFlag):

# this might not be needed/ not totally sure if firestore does this
# function: run_query_engine
# - while true loop
# - calls user_query, parse_input, get_records
# - prints results
#def run_query_engine():
