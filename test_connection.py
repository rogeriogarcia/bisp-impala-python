import os
import sys

# Add the parent directory to sys.path to allow imports from bisp_connector_lib
# This is for running this test script directly from the bisp_connector directory
current_dir = os.path.dirname(os.path.abspath(__file__))
# sys.path.insert(0, current_dir) # If bisp_connector_lib is a sibling
# If bisp_connector_lib is a subdirectory, no need to change sys.path if running from parent of bisp_connector_lib

from bisp_connector_lib import get_bisp_connection, ImpalaConnectionError

def main():
    print("--- Testing Impala Connection Package ---")
    try:
        # The .env file should be in this directory (bisp_connector) or a parent directory.
        # Alternatively, ensure environment variables are set in your system.
        conn = get_bisp_connection()
        cursor = conn.cursor()

        print("\nExecuting a sample query: SHOW DATABASES")
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()

        if databases:
            print("Databases found:")
            for db_row in databases:
                print(f"- {db_row[0]}")
        else:
            print("No databases found.")

        cursor.close()
        conn.close()
        print("\nImpala connection test successful and connection closed.")

    except ImpalaConnectionError as e:
        print(f"\nConnection Error: {e}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
    finally:
        print("\n--- Test Finished ---")

if __name__ == "__main__":
    main()