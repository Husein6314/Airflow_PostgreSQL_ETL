import pandas as pd
import re
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator

default_args = {
    'owner': 'hussein',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False
}

# Step 1: Extract
def extract_csv_data():
    data = pd.read_csv('/Users/Path/Downloads/customer_data.csv')
    data.to_csv('/tmp/raw_data.csv', index=False)
    print('Extracted Rows:', len(data))

# Step 2: Transform
def transform_data():
    data = pd.read_csv('/tmp/raw_data.csv')

    # Clean data
    data = data.dropna(subset=['FirstName', 'LastName', 'Email', 'Phone'])
    data = data.drop_duplicates(subset=['CustomerID', 'Email'])
    
    country_map = {
        'us': 'United States', 'usa': 'United States',
        'uk': 'United Kingdom', 'ca': 'Canada', 'canada': 'Canada'
    }
    data['Country'] = data['Country'].str.strip().str.lower().map(country_map).fillna(data['Country'])

    data['TotalSpent'] = pd.to_numeric(data['TotalSpent'], errors='coerce').abs()
    data = data[data['TotalSpent'].notna() & (data['TotalSpent'] > 0)]

    def is_valid_phone(val):
        return bool(re.search(r'\d{3}.*\d{4}', str(val)))
    
    data = data[data['Phone'].apply(is_valid_phone)]

    # Remove everything before and including 'x' (keep only extension)
    data['Phone'] = data['Phone'].astype(str).apply(lambda x: re.sub(r'[xX].*$', '', x))

    data.to_csv('/tmp/transformed_data.csv', index=False)
    print("Transformation complete, cleaned rows:", len(data))

# Step 3: Load
def load_data():
    hook = PostgresHook(postgres_conn_id='postgreq_conn')
    conn = hook.get_conn()
    cursor = conn.cursor()

    df = pd.read_csv('/tmp/transformed_data.csv')

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO customer_data (
                CustomerID, FirstName, LastName, Email,
                Phone, Country, TotalSpent
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (CustomerID) DO NOTHING;
        """, (
            int(row['CustomerID']), row['FirstName'], row['LastName'],
            row['Email'], row['Phone'],
            row['Country'], float(row['TotalSpent'])
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print("Data loaded successfully.")

# Define the DAG
with DAG(
    dag_id='ETL_DAG_V7',
    default_args=default_args,
    description='My complete ETL DAG with email notifications',
    start_date=datetime(2025, 5, 29),
    schedule_interval='@daily',
    catchup=False
) as dag:

    check_file = BashOperator(
        task_id='check_file',
        bash_command='if [ -f ~/Downloads/customer_data.csv ]; then echo "file exists"; else exit 1; fi'
    )

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgreq_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS customer_data (
                CustomerID    INTEGER PRIMARY KEY,
                FirstName     VARCHAR(50),
                LastName      VARCHAR(50),
                Email         VARCHAR(100),
                Phone         VARCHAR(50),
                Country       VARCHAR(50),
                TotalSpent    NUMERIC(10, 2)
            );
        """
    )

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_csv_data
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    email_success = EmailOperator(
        task_id='send_success_email',
        to='your_email@gmail.com',  # Change to your email
        subject='ETL Load Successful',
        html_content='The data has been loaded successfully into the customer_data table.',
        trigger_rule='all_success'
    )

    email_failure = EmailOperator(
        task_id='send_failure_email',
        to='your_email@gmail.com',  # Change to your email
        subject='ETL Load Failed',
        html_content='The ETL load task has failed. Please check the logs for details.',
        trigger_rule='one_failed'
    )

    # Set task dependencies
    check_file >> create_table >> extract >> transform >> load
    load >> email_success
    load >> email_failure
