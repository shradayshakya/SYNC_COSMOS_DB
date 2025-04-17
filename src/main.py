import os
import json
import argparse
from dotenv import load_dotenv
from .utils import log_info, log_success, log_error, log_stage
from .clients import get_source_target_clients
from .containers import ContainerManager
from .migration import DataMigrator

def get_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Migrate data between Azure Cosmos DB accounts")
    parser.add_argument("--source-account", default=os.environ.get("SOURCE_ACCOUNT"),
                        help="Source Cosmos DB account name")
    parser.add_argument("--target-account", default=os.environ.get("TARGET_ACCOUNT"),
                        help="Target Cosmos DB account name")
    parser.add_argument("--source-key", default=os.environ.get("SOURCE_KEY"),
                        help="Source Cosmos DB account key")
    parser.add_argument("--target-key", default=os.environ.get("TARGET_KEY"),
                        help="Target Cosmos DB account key")
    parser.add_argument("--batch-size", type=int, default=int(os.environ.get("BATCH_SIZE", "100")),
                        help="Number of documents to process in a batch")
    parser.parse_args("--sanitize", type=bool, default=os.environ.get("SANITIZE", "false").lower() == "true")
    parser.add_argument("--max-retries", type=int, default=3,
                        help="Maximum number of retries for failed operations")
    parser.add_argument("--database", help="Migrate only this specific database")
    parser.add_argument("--container", help="Migrate only this specific container (requires --database)")
    return parser.parse_args()

def migrate_container(source_db, target_db, container_id, migrator):
    """Migrate a single container"""
    try:
        # Get container properties
        properties = ContainerManager.get_container_properties(source_db, container_id)
        
        # Create container in target
        target_container = ContainerManager.create_container_if_not_exists(target_db, container_id, properties)
        
        # Get container clients
        source_container = source_db.get_container_client(container_id)
        
        # Migrate data
        result = migrator.migrate_container(source_container, target_container)
        
        # Verify migration
        verified, source_count, target_count = migrator.verify_migration(source_container, target_container)
        
        return {
            **result,
            "verified": verified,
            "source_count": source_count,
            "target_count": target_count
        }
    except Exception as e:
        log_error(f"Failed to migrate container {container_id}: {str(e)}")
        raise

def migrate_database(source_client, target_client, database_id, migrator):
    """Migrate a single database"""
    try:
        log_stage(f"Migrating database \"{database_id}\"")
        
        # Get database clients
        source_db = source_client.get_database_client(database_id)
        target_db = target_client.create_database_if_not_exists(database_id)
        
        # Get containers
        containers = ContainerManager.list_containers(source_db)
        
        results = []
        for container_id in containers:
            result = migrate_container(source_db, target_db, container_id, migrator)
            results.append({
                "container_id": container_id,
                **result
            })
        
        return results
    except Exception as e:
        log_error(f"Failed to migrate database {database_id}: {str(e)}")
        raise

def main():
    """Main entry point"""
    try:
        # Load environment variables
        load_dotenv()
        
        # Parse arguments
        args = get_args()
        
        # Initialize clients
        source_client, target_client = get_source_target_clients(
            args.source_account, args.source_key,
            args.target_account, args.target_key
        )
        
        # Initialize migrator
        migrator = DataMigrator(
            batch_size=args.batch_size,
            max_retries=args.max_retries,
            sanitize=args.sanitize
        )
        
        results = []
        
        # Handle single container migration
        if args.container:
            if not args.database:
                raise ValueError("--database is required when --container is specified")
            
            result = migrate_container(
                source_client.get_database_client(args.database),
                target_client.get_database_client(args.database),
                args.container,
                migrator
            )
            results.append({
                "database_id": args.database,
                "containers": [{
                    "container_id": args.container,
                    **result
                }]
            })
        
        # Handle single database migration
        elif args.database:
            container_results = migrate_database(
                source_client,
                target_client,
                args.database,
                migrator
            )
            results.append({
                "database_id": args.database,
                "containers": container_results
            })
        
        # Handle full account migration
        else:
            databases = source_client.list_databases()
            for database_id in databases:
                container_results = migrate_database(
                    source_client,
                    target_client,
                    database_id,
                    migrator
                )
                results.append({
                    "database_id": database_id,
                    "containers": container_results
                })
        
        # Save results
        with open("migration_summary.json", "w") as f:
            json.dump({
                "source_account": args.source_account,
                "target_account": args.target_account,
                "results": results
            }, f, indent=2)
        
        log_success("Migration completed successfully!")
        return 0
        
    except Exception as e:
        log_error(f"Migration failed: {str(e)}")
        return 1
