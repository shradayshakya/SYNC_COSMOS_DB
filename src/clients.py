from azure.cosmos import CosmosClient, exceptions
from .utils import log_info, log_success, log_error

class CosmosDBClient:
    """Handler for Cosmos DB client operations"""
    
    def __init__(self, account_name, account_key):
        """Initialize Cosmos DB client"""
        self.account_name = account_name
        self.url = f"https://{account_name}.documents.azure.com:443/"
        self.client = CosmosClient(url=self.url, credential=account_key)
    
    def test_connection(self):
        """Test the connection to Cosmos DB"""
        try:
            log_info(f"Testing connection to {self.account_name}...")
            self.client.list_databases()
            log_success(f"Successfully connected to {self.account_name}")
            return True
        except Exception as e:
            log_error(f"Failed to connect to {self.account_name}: {str(e)}")
            return False
    
    def get_database_client(self, database_id):
        """Get a database client"""
        return self.client.get_database_client(database_id)
    
    def list_databases(self):
        """List all databases in the account"""
        try:
            log_info(f"Listing databases in {self.account_name}...")
            databases = list(self.client.list_databases())
            database_ids = [db["id"] for db in databases]
            log_success(f"Found {len(database_ids)} databases: {', '.join(database_ids)}")
            return database_ids
        except Exception as e:
            log_error(f"Failed to list databases: {str(e)}")
            raise
    
    def create_database_if_not_exists(self, database_id):
        """Create a database if it does not exist"""
        try:
            log_info(f"Creating database {database_id} (if not exists)...")
            database = self.client.create_database_if_not_exists(id=database_id)
            log_success(f"Database {database_id} is ready")
            return database
        except exceptions.CosmosHttpResponseError as e:
            log_error(f"Failed to create database {database_id}: {str(e)}")
            raise

def get_source_target_clients(source_account, source_key, target_account, target_key):
    """Initialize both source and target clients"""
    try:
        source_client = CosmosDBClient(source_account, source_key)
        target_client = CosmosDBClient(target_account, target_key)
        
        # Test connections
        if not source_client.test_connection():
            raise Exception("Failed to connect to source account")
        if not target_client.test_connection():
            raise Exception("Failed to connect to target account")
        
        return source_client, target_client
    except Exception as e:
        log_error(f"Failed to initialize clients: {str(e)}")
        raise
