import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, parent_path)  # caution: path[0] is reserved for script path (or '' in REPL)

import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv
from datetime import datetime
import pytz

load_dotenv()

sydney_tz = pytz.timezone("Australia/Sydney")
sydney_time = datetime.now(sydney_tz)
date = sydney_time.strftime("%d %b %Y")


# Settings
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
USERNAME = os.getenv("GMAIL_EMAIL")
PASSWORD = os.getenv("GMAIL_APP_PASS")
TO_EMAIL = os.getenv("GMAIL_EMAIL")
SUBJECT = f"ASX Dashboard Hourly Logs: {date}"

# Read the log file
def send_email(file_path, subject):

    with open(file_path, 'r') as f:
        body = f.read()

    # Create the email
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = USERNAME
    msg['To'] = TO_EMAIL

    # Send the email
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(USERNAME, PASSWORD)
        server.send_message(msg)

    print("Email sent successfully.")

paths = ['~/Projects/ASX_Dashboard/2_hourly_scripts.log', '~/Projects/ASX_Dashboard/end_of_day_scripts.log']
subjects = [f"ASX Dashboard Hourly Logs: {date}", f"ASX Dashboard EOD Logs: {date}"]

for i in range(len(paths)):
    send_email(paths[i], subjects[i])