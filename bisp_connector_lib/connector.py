import os
from dotenv import load_dotenv, find_dotenv
from impala.dbapi import connect
from impala.hiveserver2 import HiveServer2Connection

class ImpalaConnectionError(Exception):
    """Custom exception for Impala connection errors."""
    pass

def get_bisp_connection() -> HiveServer2Connection:
    """
    Establishes a connection to Impala using environment variables.

    Environment variables required:
    - BISP_HOST: Hostname or IP address of the Impala server.
    - BISP_PORT: Port number for the Impala service (default: 21051).
    - BISP_DATABASE: Name of the database to connect to.
    - BISP_USER: Username for authentication.
    - BISP_PASSWORD: Password for authentication.
    - BISP_AUTH_MECHANISM: Authentication mechanism (default: 'PLAIN').
    - BISP_USE_SSL: 'True' or 'False' to enable/disable SSL (default: 'True').
    - BISP_CA_CERT_PATH: Absolute path to the CA certificate file (required if SSL is enabled).

    Returns:
        HiveServer2Connection: A connection object to Impala.

    Raises:
        ImpalaConnectionError: If any required environment variable is missing
                            or if the connection fails.
    """
    # --- Debugging Start ---
    print("--- Debugging BISP Connector ---")
    env_path = find_dotenv()
    print(f"Attempting to load .env file from: {env_path if env_path else 'No .env file found by find_dotenv()'}")
    
    # Try with override=True to ensure .env takes precedence
    loaded_dotenv = load_dotenv(env_path, override=True) 
    # If you want to test without override first, keep the original:
    # loaded_dotenv = load_dotenv(env_path)
    print(f".env file loaded: {loaded_dotenv}")
    
    print(f"Current working directory: {os.getcwd()}")
    # --- Debugging End ---

    required_env_vars = [
        'BISP_HOST', 'BISP_DATABASE', 'BISP_USER', 'BISP_PASSWORD'
    ]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        raise ImpalaConnectionError(f"Missing required environment variables: {', '.join(missing_vars)}")

    bisp_host = os.getenv('BISP_HOST')
    bisp_port = int(os.getenv('BISP_PORT', 21051))
    bisp_database = os.getenv('BISP_DATABASE')
    bisp_user = os.getenv('BISP_USER')
    bisp_password = os.getenv('BISP_PASSWORD')
    auth_mechanism = os.getenv('BISP_AUTH_MECHANISM', 'PLAIN')
    use_ssl = os.getenv('BISP_USE_SSL', 'True').lower() == 'true'
    ca_cert_path = os.getenv('BISP_CA_CERT_PATH')
    
    # --- Debugging Start ---
    print(f"Value of BISP_CA_CERT_PATH from os.getenv: {ca_cert_path}")
    if ca_cert_path:
        # Check if it's an absolute path, if not, resolve it based on CWD
        if not os.path.isabs(ca_cert_path):
            abs_ca_cert_path = os.path.join(os.getcwd(), ca_cert_path)
            print(f"Relative BISP_CA_CERT_PATH '{ca_cert_path}' resolved to absolute path: {abs_ca_cert_path}")
            print(f"Does this resolved absolute path exist? {os.path.exists(abs_ca_cert_path)}")
        else:
            print(f"Absolute BISP_CA_CERT_PATH: {ca_cert_path}")
            print(f"Does this absolute path exist? {os.path.exists(ca_cert_path)}")
    else:
        print("BISP_CA_CERT_PATH is not set.")
    print("--- End Debugging BISP Connector ---")
    # --- Debugging End ---

    if use_ssl and not ca_cert_path:
        raise ImpalaConnectionError("BISP_CA_CERT_PATH is required when BISP_USE_SSL is True.")
    if use_ssl and not os.path.exists(ca_cert_path):
         raise ImpalaConnectionError(f"CA certificate file not found at: {ca_cert_path}")


    connection_params = {
        'host': bisp_host,
        'port': bisp_port,
        'database': bisp_database,
        'user': bisp_user,
        'password': bisp_password,
        'auth_mechanism': auth_mechanism,
        'use_ssl': use_ssl,
    }

    if use_ssl:
        connection_params['ca_cert'] = ca_cert_path

    try:
        print("Attempting to connect to Impala...")
        print(f"  Host: {bisp_host}, Port: {bisp_port}, DB: {bisp_database}")
        print(f"  User: {bisp_user}, Auth: {auth_mechanism}, SSL: {use_ssl}")
        if use_ssl:
            print(f"  CA Cert: {ca_cert_path}")
        conn = connect(**connection_params)
        print("Successfully connected to Impala!")
        return conn
    except Exception as e:
        error_msg = f"Failed to connect to Impala: {e}"
        print(error_msg)
        raise ImpalaConnectionError(error_msg)

if __name__ == '__main__':
    # This part is for direct testing of the connector.py script
    # It requires a .env file in the same directory or parent directories
    # with the bisp_... variables set.
    try:
        connection = get_bisp_connection()
        cursor = connection.cursor()
        print("\nExecuting a sample query: SHOW TABLES")
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        if tables:
            print("Tables found:")
            for table in tables:
                print(f"- {table[0]}")
        else:
            print("No tables found in the database.")
        cursor.close()
        connection.close()
        print("Connection closed.")
    except ImpalaConnectionError as e:
        print(f"Connection test failed: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during testing: {e}")
