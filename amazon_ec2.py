import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
import os
import requests
import csv
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import numpy as np 
import pandas as pd
from dotenv import load_dotenv
import papermill as pm
import airflow.hooks.S3_hook
from airflow.operators.postgres_operator import PostgresOperator
import airflow.hooks.postgres_hook

dotenv_local_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_local_path, verbose=True)

# ----------------------------------------------------------------------------------------------------
# Setup DAG

default_args = {
	'owner':'amit',
	#'start_date': datetime(2020,11,4,0),
	'start_date': datetime.now(),
	'retries': 0,
	'retry_delay': timedelta(minutes=1),
}

dag = DAG(
	'amazon_ec2',
	default_args = default_args,
	description = 'amazon order history',
	#schedule_interval = timedelta(hours=1),
	catchup = False,
	max_active_runs = 1,
	)

# ----------------------------------------------------------------------------------------------------
# Connect to Postgres on AWS RDS and create schema

t1 = PostgresOperator(
	task_id = 'drop_schema_postgres',
	postgres_conn_id = 'amazon_order_history_aws',
	sql = '''
	DROP SCHEMA amazon CASCADE;
	CREATE SCHEMA amazon;''',
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Read input csv from AWS S3 into pandas, peform ETL, export final dataframe to PostgreSQL on AWS RDS

csv_input_path = '/Users/amit/Coding/Projects/AmazonOrderHistoryAirflowAWS_EC2/amazon_purchases.csv'

def etl_csv():

	# Read csv
	df = pd.read_csv(csv_input_path, parse_dates=['Order Date', 'Shipment Date'])

	# Rename columns to remove spaces.
	df.columns = df.columns.str.replace(' ', '')

	# Rename specific columns.
	df = df.rename(columns={'CarrierName&TrackingNumber':'Carrier', 'ItemSubtotalTax': 'Tax', 
		'ShipmentDate':'ShipDate'})

	# Drop Website column
	del df['Website']

	# Replace NaN
	df.Category.fillna('unknown', inplace = True)
	df.Condition.fillna('unknown', inplace = True)
	df.Carrier.fillna('unknown', inplace = True) 

	# Remove $ and , from price columns.
	df['ListPricePerUnit'] = df['ListPricePerUnit'].str.replace('$','').str.replace(',','')
	df['PurchasePricePerUnit'] = df['PurchasePricePerUnit'].str.replace('$','').str.replace(',','')
	df['ItemSubtotal'] = df['ItemSubtotal'].str.replace('$','').str.replace(',','')
	df['Tax'] = df['Tax'].str.replace('$','').str.replace(',','')
	df['ItemTotal'] = df['ItemTotal'].str.replace('$','').str.replace(',','')

	# Convert price columns to float.
	df['ListPricePerUnit'] = df['ListPricePerUnit'].astype(float)
	df['PurchasePricePerUnit'] = df['PurchasePricePerUnit'].astype(float)
	df['ItemSubtotal'] = df['ItemSubtotal'].astype(float)
	df['Tax'] = df['Tax'].astype(float)
	df['ItemTotal'] = df['ItemTotal'].astype(float)

	# Drop rows with zero prices.
	df = df[df.ListPricePerUnit != 0]
	df = df[df.PurchasePricePerUnit != 0]
	df = df[df.ItemSubtotal != 0]
	df = df[df.ItemTotal != 0]

	# Extract year, month, & day and store them in columns in df_main
	df['OrderYear'] = df['OrderDate'].dt.year
	df['OrderMonth'] = df['OrderDate'].dt.month
	df['OrderDay'] = df['OrderDate'].dt.day
	df['OrderDayIndex'] = df['OrderDate'].dt.dayofweek
	df['OrderDayName'] = df['OrderDate'].dt.day_name()

	# Drop rows where year = 2020
	df = df.drop(df[df['OrderDate'].dt.year == 2020].index)

	# Combine carriers to eliminate repitition
	df['Carrier'] = df['Carrier'].replace('FEDEX', 'FedEx')
	df['Carrier'] = df['Carrier'].replace('SMARTPOST', 'FedEx SmartPost')
	df['Carrier'] = df['Carrier'].replace('Mail Innovations','UPS Mail Innovations')
	df['Carrier'] = df['Carrier'].replace('UPS MI','UPS Mail Innovations')
	df['Carrier'] = df['Carrier'].replace('US Postal Service','USPS')
	df['Carrier'] = df['Carrier'].replace('DHL Global Mail','DHL')
	df['Carrier'] = df['Carrier'].replace('US Postal Service','USPS')
	df['Carrier'] = df['Carrier'].replace('AMZN_US', 'AMZN')
	mail = ['USPS', 'UPS', 'UPS Mail Innovations', 'FedEx', 'FedEx SmartPost', 'DHL', 'AMZN']
	df.loc[~df.Carrier.isin(mail), 'Carrier'] = 'Other'

	# Combine categories
	df['Category'] = df['Category'].replace(['NOTEBOOK_COMPUTER','COMPUTER_DRIVE_OR_STORAGE','RAM_MEMORY','TABLET_COMPUTER','MONITOR','COMPUTER_COMPONENT', 'FLASH_MEMORY', 'SOFTWARE', 'INK_OR_TONER', 'COMPUTER_INPUT_DEVICE', 'CABLE_OR_ADAPTER', 'NETWORKING_DEVICE', 'KEYBOARDS', 'COMPUTER_ADD_ON', 'NETWORKING_ROUTER','MEMORY_READER','WIRELESS_ACCESSORY','SCANNER','PRINTER'],'COMPUTER')
	df['Category'] = df['Category'].replace(['HEADPHONES','SPEAKERS','BATTERY','MULTIFUNCTION_DEVICE','ELECTRONIC_CABLE','SURVEILANCE_SYSTEMS','SECURITY_CAMERA','WATCH','CONSUMER_ELECTRONICS','CE_ACCESSORY','ELECTRONIC_ADAPTER','ELECTRIC_FAN','CAMCORDER','HANDHELD_OR_PDA','TUNER','AMAZON_BOOK_READER','CELLULAR_PHONE','POWER_SUPPLIES_OR_PROTECTION','CAMERA_OTHER_ACCESSORIES','CHARGING_ADAPTER'],'ELECTRONICS')
	df['Category'] = df['Category'].replace(['HAIR_STYLING_AGENT','PERSONAL_CARE_APPLIANCE','PROFESSIONAL_HEALTHCARE','HEALTH_PERSONAL_CARE','SHAMPOO','VITAMIN','ABIS_DRUGSTORE','BEAUTY'],'HEALTH_BEAUTY')
	df['Category'] = df['Category'].replace(['KITCHEN','SEEDS_AND_PLANTS','HOME_LIGHTING_ACCESSORY','BOTTLE','OUTDOOR_LIVING','ELECTRIC_FAN','TABLECLOTH','COFFEE_MAKER','HOME_BED_AND_BATH','HOME_LIGHTING_AND_LAMPS','SMALL_HOME_APPLIANCES'],'HOME')
	df['Category'] = df['Category'].replace(['SHOES','PANTS','SHIRT','SHORTS','OUTERWEAR','SWEATSHIRT','HAT', 'SOCKSHOSIERY','UNDERWEAR','TECHNICAL_SPORT_SHOE'],'APPAREL')
	df['Category'] = df['Category'].replace(['OUTDOOR_RECREATION_PRODUCT','SPORTING_GOODS'],'SPORTS_OUTDOOR')
	df['Category'] = df['Category'].replace(['TEA','COFFEE'],'GROCERY')
	df['Category'] = df['Category'].replace(['AUTO_PART','HARDWARE','AUTO_ACESSORY','PRECISION_MEASURING','BUILDING_MATERIAL','AUTO_ACCESSORY'],'TOOLS')
	df['Category'] = df['Category'].replace(['WRITING_INSTRUMENT','PAPER_PRODUCT','BACKPACK','CARRYING_CASE_OR_BAG','CE_CARRYING_CASE_OR_BAG','OFFICE_PRODUCTS'],'OFFICE')
	df['Category'] = df['Category'].replace(['ABIS_DVD','TOYS_AND_GAMES','ABIS_MUSIC','DOWNLOADABLE_VIDEO_GAME','ART_AND_CRAFT_SUPPLY'],'ENTERTAINMENT')
	df['Category'] = df['Category'].replace(['ABIS_BOOK'],'BOOKS')
	df['Category'] = df['Category'].replace(['ABIS_GIFT_CARD'],'GIFT_CARD')
	df['Category'] = df['Category'].replace(['AV_FURNITURE','CELLULAR_PHONE_CASE','PHONE_ACCESSORY','PET_SUPPLIES','ACCESSORY','BAG','ACCESSORY_OR_PART_OR_SUPPLY'],'OTHER')
	df['Category'] = df['Category'].replace(['','unknown'],'UNKNOWN')

	# Reduce Sellers
	df.loc[~df.Seller.isin(['Amazon.com']), 'Seller'] = 'ThirdParty'
	df.loc[df.Seller.isin(['Amazon.com']), 'Seller'] = 'Amazon'

	# Final dataframe
	df

	# Connect to PostgreSQL AWS RDS using sqlalchemy
	engine = create_engine("postgres://" + os.environ.get("AWS_POSTGRES_USER") + ":" + os.environ.get("AWS_POSTGRES_PASSWORD") + "@" + "amazon.coqoinqxklrf.us-east-1.rds.amazonaws.com:5432/postgres")

	# Export df to sql using df.to_sql
	df.to_sql('purchases_aws', con=engine, if_exists = 'replace', index=False, schema='amazon')

t2 = PythonOperator(
	task_id = 'etl_amazon_purchases.csv',
	python_callable = etl_csv,
	provide_context = False,
	dag = dag
)

# ----------------------------------------------------------------------------------------------------
# Run Jupyter Notebook locally

notebook_in_path = '/Users/amit/Coding/Projects/AmazonOrderHistoryAirflowAWS_EC2/AmazonOrderHistoryAirflowAWS_EC2_input.ipynb'
notebook_out_path = '/Users/amit/Coding/Projects/AmazonOrderHistoryAirflowAWS_EC2/AmazonOrderHistoryAirflowAWS_EC2_output.ipynb'

def run_notebook():
	pm.execute_notebook(notebook_in_path,notebook_out_path)


t3 = PythonOperator(
	task_id = 'run_notebook',
	python_callable = run_notebook,
	provide_context = False,
	dag = dag
)


t1 >> t2 >> t3
