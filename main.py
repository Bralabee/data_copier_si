import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE
from email import encoders
import pyodbc
import pandas as pd
from hydra.core.utils import run_job
from sqlalchemy import create_engine
from prefect import task, flow
from omegaconf import DictConfig, OmegaConf
from hydra import compose, initialize
import datetime

from helper import load_config
from dotenv import load_dotenv

config = load_config()

load_dotenv()

QUERY_FILE = os.environ.get("query_path")
SERVER = os.environ.get("SERVER")
DATABASE = os.environ.get("DATABASE")
UID = os.environ.get("UID")
PWD = os.environ.get("PWD")


# @task(name="Get yesterday date")
def get_yesterday_date():
    return (datetime.date.today() - datetime.timedelta(days=1)).strftime('%d-%m-%Y')


yesterday = get_yesterday_date()


# @task(name="Load data")
def read_query(query_file: str) -> str:
    with open(query_file) as f:
        query = f.read()
    return query


query = read_query(QUERY_FILE)
print(query)


# @task
def get_data(query: str, server: str, database: str, username: str, password: str) -> pd.DataFrame:
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")
    data = pd.read_sql_query(query, engine)
    return data


#@task
def save_csv(data: pd.DataFrame, output_dir: str):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    file_name = f"{yesterday}_single_line_booking_raw.csv"
    output_path = os.path.join(output_dir, file_name)
    data.to_csv(output_path, index=False)


# #@task
# def send_email(smtp_server: str, smtp_port: int, smtp_user: str, smtp_password: str, from_address: str,
#                to_addresses: list, subject: str, body: str, attachment_path: str):
#     msg = MIMEMultipart()
#     msg['From'] = from_address
#     msg['To'] = COMMASPACE.join(to_addresses)
#     msg['Subject'] = subject
#     msg.attach(MIMEText(body, 'html'))
#     with open(attachment_path, "rb") as attachment:
#         part = MIMEBase("application", "octet-stream")
#         part.set_payload(attachment.read())
#         encoders.encode_base64(part)
#         part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(attachment_path)}"')
#         msg.attach(part)
#     smtp_server = smtplib.SMTP(smtp_server, smtp_port)
#     smtp_server.ehlo()
#     smtp_server.starttls()
#     smtp_server.login(smtp_user, smtp_password)
#     smtp_server.sendmail(from_address, to_addresses, msg.as_string())
#     smtp_server.quit()
#
#
# #@task
# def run_job(query_file: str, output_dir:
# str, server: str, database: str, username: str, password: str, smtp_server: str, smtp_port: int, smtp_user: str,
#             smtp_password: str, from_address: str, to_addresses: list, subject: str, body: str):
#     query = read_query(query_file)
#     data = get_data(query, server, database, username, password)
#     save_csv(data, output_dir)
#     attachment_path = os.path.join(output_dir, "data.csv")
#     send_email(smtp_server, smtp_port, smtp_user, smtp_password, from_address, to_addresses, subject, body,
#                attachment_path)

def run_job(query_file: str, output_dir: DictConfig,
server: str, database: str, username: str, password: str): #smtp_server: str, smtp_port: int, smtp_user: str,smtp_password: str, from_address: str, to_addresses: list, subject: str, body: str):
    query = read_query(query_file)
    data = get_data(query, server, database, username, password)
    save_csv(data, output_dir)
    #attachment_path = os.path.join(output_dir, "data.csv")
    #send_email(smtp_server, smtp_port, smtp_user, smtp_password, from_address, to_addresses, subject, body,attachment_path)


#@flow(name="SQL Job")
def run_sql_job():
    run_job(
        query_file=QUERY_FILE,
        output_dir=config.output_dir,
        server=SERVER,
        database=DATABASE,
        username=UID,
        password=PWD
        # smtp_server=config.smtp_server,
        # smtp_port=config.smtp_port,
        # smtp_user=config.smtp_user,
        # smtp_password=config.smtp_password,
        # from_address=config.from_address,
        # to_addresses=config.to_addresses,
        # subject=config.subject,
        # body=config.body,
    )


if __name__ == "__main__":
    run_sql_job()
