import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, parent_path)  # caution: path[0] is reserved for script path (or '' in REPL)

from utils.AWS_functions import delete_folder_content
from dotenv import load_dotenv

load_dotenv()
s3_folder = os.getenv("AWS_S3_FOLDER")
delete_folder_content(s3_folder)