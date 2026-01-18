from azure.data.tables import TableServiceClient
from azure.core.exceptions import AzureError, HttpResponseError


class PythonAzureClient:
    def __init__(self):
        self.table_service = None
        self.table_clients = {}

    def create_conn(self, conn_str):
        self.table_service = TableServiceClient.from_connection_string(conn_str)

    def range_search(self, tbl_name, part, low_key, high_key):
        table_client = self.table_clients.get(tbl_name, None)
        if table_client is None:
            table_client = self.table_service.get_table_client(tbl_name)
            self.table_clients[tbl_name] = table_client
        filters = [
            "PartitionKey eq '{}'".format(part),
            "RowKey ge '{}'".format(low_key),
            "RowKey le '{}'".format(high_key),
        ]
        try:
            result = list(table_client.query_entities(" and ".join(filters)))
            return result
        
        except HttpResponseError as http_err:
            # For HTTP/REST errors from Azure (status code, error code, etc.)
            print(f"HTTP response error while querying table '{tbl_name}':")
            print(f"  status_code={http_err.status_code}, error_code={http_err.error_code}")
            print(f"  message={http_err.message}")
             # You can inspect http_err.response if needed
            raise  # re-raise, or handle as needed

        except AzureError as azure_err:
            # Catches other Azure-related exceptions, e.g. auth issues, timeouts
            print(f"Azure error while querying table '{tbl_name}': {azure_err}")
            raise

        except Exception as e:
            # Fallback for anything else (Python/system errors)
            print(f"Unexpected error while querying table '{tbl_name}': {e}")
            raise

    def point_query(self, tbl_name, part, row_key):
        table_client = self.table_clients.get(tbl_name)
        if table_client is None:
            table_client = self.table_service.get_table_client(tbl_name)
            self.table_clients[tbl_name] = table_client

        filter_str = "PartitionKey eq '{}' and RowKey eq '{}'".format(part, row_key)
        try:
            entities = list(table_client.query_entities(filter_str))
            return entities[0] if entities else None
        except HttpResponseError as http_err:
            print(f"HTTP response error while querying table '{tbl_name}':")
            print(f"  status_code={http_err.status_code}, error_code={http_err.error_code}")
            print(f"  message={http_err.message}")
            raise
        except AzureError as azure_err:
            print(f"Azure error while querying table '{tbl_name}': {azure_err}")
            raise
        except Exception as e:
            print(f"Unexpected error while querying table '{tbl_name}': {e}")
            raise



        # result = list(table_client.query_entities(" and ".join(filters)))
