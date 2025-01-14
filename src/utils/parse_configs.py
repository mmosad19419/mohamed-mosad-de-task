# Parsing the configration file
with open('./config.json') as config_file:
    configs = json.load(config_file)

# retrieve API credientials
NYT_BOOKS_API_KEY = configs["API"]["NYT_BOOKS_API_KEY"]
NYT_BOOKS_API_SECRET = configs["API"]["NYT_BOOKS_API_SECRET"]
NYT_BOOKS_API_ENDPOINT = configs["API"]["NYT_API_ENDPOINT"]