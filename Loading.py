#Importing Necessary libraries
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient
from dotenv import load_dotenv
import os


# Data Laoding
def run_loading():
    # Loading the dataset
    data = pd.read_csv(r'clean_data.csv')
    products = pd.read_csv(r'product.csv')
    staff = pd.read_csv(r'staff.csv')
    customers = pd.read_csv(r'customers.csv')
    transaction = pd.read_csv(r'transaction.csv')

    # Loading the environment variables from the .env files
    load_dotenv()

    # Clear cached variables
    os.environ.pop('CONNECT_STR', None)
    os.environ.pop('CONTAINER_NAME', None)

    # Reload the .env file
    load_dotenv(override=True)
    connect_str = os.getenv('CONNECT_STR')
    container_name = os.getenv('CONTAINER_NAME')

    #print(connect_str)  # Ensure changes are reflected
    #print(container_name)


    from azure.storage.blob import BlobServiceClient
    from azure.core.exceptions import ResourceExistsError
    from dotenv import load_dotenv
    import os

    # Load the environment variables
    load_dotenv()

    # Get the connection string and container name
    connect_str = os.getenv('CONNECT_STR')
    container_name = os.getenv('CONTAINER_NAME')

    if not connect_str:
        raise ValueError("Azure Blob Storage connection string not found in environment variables.")

    # Create a BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Check and create the container if it doesn't exist
    try:
        container_client = blob_service_client.create_container(container_name)
        print(f"Container '{container_name}' created.")
    except ResourceExistsError:
        print(f"Container '{container_name}' already exists.")
        container_client = blob_service_client.get_container_client(container_name)

    # Load data to Azure Blob Storage
    files = [
        (data, 'rawdata/cleaned_zipco_transaction_data.csv'),
        (products, 'cleaneddata/product.csv'),
        (customers, 'cleaneddata/customers.csv'),
        (staff, 'cleaneddata/staff.csv'),
        (transaction, 'cleaneddata/transaction.csv')
    ]

    # Upload the files to Azure Blob Storage
    for file, blob_name in files:
        blob_client = container_client.get_blob_client(blob_name)
        output = file.to_csv(index=False)
        blob_client.upload_blob(output, overwrite=True)
        print(f'{blob_name} loaded into Azure Blob Storage')

