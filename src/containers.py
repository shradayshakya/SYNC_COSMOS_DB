from azure.cosmos import PartitionKey, exceptions
from .utils import log_info, log_success, log_error, log_warning

class ContainerManager:
    """Handler for Cosmos DB container operations"""
    
    @staticmethod
    def get_container_properties(database, container_id):
        """Get container properties including partition key and indexing policy"""
        try:
            log_info(f"Reading properties for container \"{container_id}\"...")
            container = database.get_container_client(container_id)
            properties = container.read()
            
            # Extract partition key path and indexing policy
            partition_key_path = properties.get("partitionKey", {}).get("paths", ["/id"])[0]
            indexing_policy = properties.get("indexingPolicy", None)
            
            # Get throughput info (if dedicated throughput is set)
            throughput_info = None
            try:
                offer = container.read_offer()
                if offer:
                    throughput_info = offer.get("content", {}).get("offerThroughput")
            except Exception as e:
                log_warning(f"Could not read throughput for container \"{container_id}\": {str(e)}")
                # Container might be using shared throughput

            log_info(
                f"Container \"{container_id}\" properties: "
                f"partition key = \"{partition_key_path}\", throughput = {throughput_info or 'shared'}"
            )
            return {
                "partition_key_path": partition_key_path,
                "indexing_policy": indexing_policy,
                "throughput": throughput_info
            }
        except Exception as e:
            log_error(f"Failed to get container properties for \"{container_id}\": {str(e)}")
            return {
                "partition_key_path": "/id",  # Safe fallback
                "indexing_policy": None,
                "throughput": None
            }

    @staticmethod
    def create_container_if_not_exists(database, container_id, properties):
        """Create container with specified properties"""
        try:
            log_info(f"Creating container \"{container_id}\" (if not exists)...")
            
            # Define partition key
            partition_key = PartitionKey(path=properties["partition_key_path"])
            container_options = {
                "id": container_id,
                "partition_key": partition_key
            }

            if properties["indexing_policy"]:
                container_options["indexing_policy"] = properties["indexing_policy"]
            
            # Add throughput if defined
            throughput_params = {}
            if properties["throughput"]:
                throughput_params["offer_throughput"] = properties["throughput"]
            
            container = database.create_container_if_not_exists(
                **container_options,
                **throughput_params
            )

            log_success(f"Container \"{container_id}\" created or verified successfully.")
            return container
        except exceptions.CosmosHttpResponseError as e:
            log_error(f"Failed to create container \"{container_id}\": {str(e)}")
            raise

    @staticmethod
    def list_containers(database):
        """List all containers in a database"""
        try:
            log_info(f"Listing containers in database \"{database.id}\"...")
            containers = list(database.list_containers())
            container_ids = [container["id"] for container in containers]
            log_success(f"Found {len(container_ids)} containers: {', '.join(container_ids)}")
            return container_ids
        except Exception as e:
            log_error(f"Failed to list containers in database \"{database.id}\": {str(e)}")
            raise