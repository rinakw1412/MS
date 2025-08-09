Financial Transaction Analytics Pipeline

Overview :
This project is a part of Analytics Engineer test for Mandiri Sekuritas.
It demonstrates an ETL workflow using dbt and PostgreSQL to process financial transactions data, 
which ingests raw source files, applies cleaning, transformation and aggregation and produces analytics ready marts for reporting

Assumptions :
-  The project does not cover deployment phase.
-  Raw files used in this projects are the provided example files. 
-  Account data are active Account (no dormant or closed accounts)
-  Account.Balance are in USD
-  Deduplication on Transactions data based on transaction_id only
-  PostgreSQL stores Raw and Analytical data.
-  Transactions.currency only accept IDR, USD, JPY, EUR, AUD
   

Data Model : 
<img width="1806" height="472" alt="image" src="https://github.com/user-attachments/assets/e2a87184-1a8c-4d80-8782-914c1ade6ad4" />


How to set up the project : 
1. Prerequisites
    - Python 3.9+ installed
    - PostgreSQL database (locally installed or cloud)
    - Airflow installed
    - dbt cloud account

2. Database setup :
	- Connection 
	   For this project, I used Supabase (https://supabase.com/) to host cloud PostgreSQL database. 
	   Connection details are as follows :
		type: postgres
		host : aws-0-ap-southeast-1.pooler.supabase.com
		user : postgres.zrszjmngrrdaxjwxlfed
		password : Super123
		port: 5432
		dbname : postgres
		schema : dbt_rkurniawati (target schema)
	- RAW tables creation :
		Run the DDL for RAW tables (file : "DDL - RAW tables.txt")

3. dbt setup :
    - Download the script from the following repositories : 
        https://github.com/rinakw1412/MS/
    - On dbt cloud, Copy models & .yml files to the respective folders of dbt project

How to run the pipeline :
1. Run the Airflow DAG : 
- Airflow DAG runs pipeline in the following order:
        load_customers » load_accounts » load_transactions
- Execute the following command to run the ingestion DAG :
    python ingestion.py    

2. Run the dbt project :
- On dbt cloud, execute the following command :
    dbt build
- Transformed data will appear in target schema

3. dbt documentation :
- On dbt cloud, execute the following command :
    dbt docs generate

Challenges encountered during the test :
1. Airflow + Docker setup failed locally → tested ingestion scripts in Jupyter instead.
2. Initially used MySQL → switched to PostgreSQL for easier dbt integration.
3. Chose not to implement SCD to keep scope simple.


