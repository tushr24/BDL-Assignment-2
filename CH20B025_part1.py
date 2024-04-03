import re, os
import shutil
import random
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# DAG -> default arguements
default_args = {'owner': 'airflow','start_date': datetime(2024, 1, 1),}

# Defining DAG
with DAG(
    dag_id='a2_pipeline_1',
    default_args=default_args,
    schedule_interval=None,  # Run manually
) as dag:

    # VARIABLES
    # year as a placeholder for the year
    year = '2020'  

    # filename to store the HTML content
    html_filename = f'/tmp/ncei_data_{year}.html'

    # number of data files to select
    num_files_to_fetch = 10 

    # download directory and the base URL
    base_url = f"https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}/"
    download_dir = '/tmp/ncei_data'

    # FETCH PAGE 
    # Fetch page and save to a file
    fetch_page = BashOperator(
        task_id='fetch_page',
        bash_command=f"curl -s -o {html_filename} {base_url}; ls; pwd",
    )

    # ALL FILENAMES 
    # function to extract CSV filenames
    def extract_csv_filenames(html_path, ti):
        """Extracts CSV filenames from the provided HTML file path."""
        print(os.getcwd())

        with open(html_path, 'r') as file:
            html_content = file.read()

        # Extract text within quotation marks for anchor tags with href ending in '.csv'
        csv_filenames = re.findall(r'<a href="([^"]+\.csv)">', html_content)

        ti.xcom_push(key='selected_files', value=csv_filenames)

    # Extract CSV filenames from the saved HTML
    csv_names = PythonOperator(
        task_id='extract_csv_names',
        python_callable=extract_csv_filenames,
        op_args=[html_filename],
        provide_context=True,
    )

    # SELECT RANDOM FILES 
    # Define function to select random CSV files
    def select_random_files(num_files, ti):
        """Selects a random subset of CSV filenames."""

        csv_filenames = ti.xcom_pull(task_ids='extract_csv_names', key='selected_files')
        if int(num_files) > len(csv_filenames):
            raise ValueError(f"Cannot select {num_files} files from {len(csv_filenames)} available files")

        # selected_files = random.sample(csv_filenames, int(num_files))
        ti.xcom_push(key='selected_files', value=random.sample(csv_filenames, num_files))

    # Select random CSV files
    select_files = PythonOperator(
        task_id='select_files',
        python_callable=select_random_files,
        op_args=[num_files_to_fetch],
        provide_context=True,
    )

    # DOWNLOAD INDIVIDUAL FILES
    # Define function to fetch data files
    def fetch_data_files(base_url, download_dir, ti):
        """Fetches individual data files from the base URL."""

        selected_files = ti.xcom_pull(task_ids='select_files', key='selected_files')
        os.makedirs(download_dir, exist_ok=True)  # Create directory if it doesn't exist
        command = ""
        for filename in selected_files:
            download_path = os.path.join(download_dir, filename)
            command += f"curl -s -o {download_path} {base_url}{filename}; "
        os.system(command)

    # Fetch individual data files
    fetch_data_tasks = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_files,
        op_args=[base_url,download_dir,],
        provide_context=True,
    )

# ZIP FILES 
    def zip_files():
        directory = '/tmp/ncei_data/' 
        zip_file = f"ncei_data_{year}"
        os.chdir('/opt/airflow/dags')
        shutil.make_archive(zip_file, 'zip', root_dir=directory)
        shutil.rmtree(directory)  # Delete the original folder
        os.remove(os.path.join('/tmp', zip_file+'.html')) # Remove the html file

    zip_task = PythonOperator(
        task_id="zip_files",
        python_callable=zip_files,
    )

    # FINAL FLOW 
    fetch_page >> csv_names >> select_files >> fetch_data_tasks >> zip_task  