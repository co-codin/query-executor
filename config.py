from os import path

from dotenv import load_dotenv

# Find .env file
basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, ".env"))
