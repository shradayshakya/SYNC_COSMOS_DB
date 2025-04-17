import time
import json
from tqdm import tqdm
from azure.cosmos import exceptions
from .utils import (
    log_info, log_success, log_error, log_stage,
    format_time, format_number
)
from .sanitizer import sanitize_document_recursive

class DataMigrator:
    """Handles the migration of data between Cosmos DB containers"""

    def __init__(self, batch_size=100, max_retries=3, sanitize=False):
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.sanitize = sanitize

    def _get_partition_key_path(self, container):
        """Get the partition key path(s) for a container"""
        paths = container.read()["partitionKey"]["paths"]
        return [path.strip("/") for path in paths]

    def _get_partition_key_value(self, item, pk_paths):
        """Extract partition key values from the document based on partition key paths"""
        pk_values = []
        for pk_path in pk_paths:
            try:
                value = item
                keys = pk_path.split("/")
                for key in keys:
                    value = value.get(key)  # Use .get() to avoid KeyError
                    if value is None:
                        break  # If the value is None, stop traversing
                pk_values.append(value)
            except (KeyError, TypeError) as e:
                log_error(f"Error extracting partition key value for {pk_path} from item {repr(item)}: {e}")
                pk_values.append(None)
        return pk_values

    def migrate_container(self, source_container, target_container):
        """Migrate documents from source container to target container"""
        try:
            start_time = time.time()
            log_stage(f"Starting migration for container \"{source_container.id}\"")

            # Get partition key paths for source and target containers
            source_pk_paths = self._get_partition_key_path(source_container)
            target_pk_paths = self._get_partition_key_path(target_container)

            # Ensure source and target containers have the same partition key paths
            if source_pk_paths != target_pk_paths:
                raise ValueError(
                    f"Partition key mismatch in container '{source_container.id}':\n"
                    f"  • Source: {source_pk_paths}\n"
                    f"  • Target: {target_pk_paths}\n"
                    f"Aborting migration — partition key paths must match."
                )

            # Count total documents in the source container
            try:
                count_query = "SELECT VALUE COUNT(1) FROM c"
                total_items = list(source_container.query_items(
                    query=count_query,
                    enable_cross_partition_query=True
                ))[0]
            except Exception as e:
                log_error(f"Failed to count items: {str(e)}")
                total_items = 0

            log_info(f"Found {format_number(total_items)} items to process")

            inserted = updated = skipped = errors = 0
            continuation_token = None

            with tqdm(total=total_items, desc="Migrating items", unit="docs") as pbar:
                while True:
                    query_iterable = source_container.query_items(
                        query="SELECT * FROM c",
                        enable_cross_partition_query=True,
                        max_item_count=self.batch_size
                    )
                    page_iterator = query_iterable.by_page(continuation_token)

                    try:
                        page = next(page_iterator)
                    except StopIteration:
                        break

                    items = list(page)
                    for item in items:
                        item_id = item.get("id")
                        if not item_id:
                            log_error("Skipping item with missing 'id'")
                            errors += 1
                            pbar.update(1)
                            continue

                        pk_values = self._get_partition_key_value(item, target_pk_paths)

                        if any(pk_value is None or isinstance(pk_value, (dict, list)) or pk_value in ["", None] for pk_value in pk_values):
                            log_error(
                                f"Skipping item {item_id}: invalid or missing partition key(s) '{target_pk_paths}'\n"
                                f"  Values: {repr(pk_values)}\n"
                                f"  Full item: {json.dumps(item)}"
                            )
                            errors += 1
                            pbar.update(1)
                            continue

                        # Ensure partition key exists in body
                        for pk_path, pk_value in zip(target_pk_paths, pk_values):
                            if pk_path not in item:
                                item[pk_path] = pk_value

                        # Try to read from target container with multiple partition keys
                        for attempt in range(1, self.max_retries + 1):
                            try:
                                target_doc = target_container.read_item(
                                    item=item_id,
                                    partition_key=pk_values if len(pk_values) > 1 else pk_value[0]
                                )

                                # Remove system fields (_etag, _rid, _self, _ts)
                                sanitized_target_doc = remove_system_fields(target_doc)
                                sanitized_item = remove_system_fields(item)

                                # Compare the content excluding system fields
                                if sanitized_target_doc == sanitized_item:
                                    skipped += 1
                                else:
                                    item_to_write = item.copy()
                                    if self.sanitize:
                                        item_to_write = sanitize_document_recursive(item_to_write)

                                    # Write to Cosmos DB (replace the existing item)
                                    target_container.replace_item(item=item_id, body=item_to_write)
                                    updated += 1

                            except exceptions.CosmosResourceNotFoundError:
                                item_to_write = item.copy()
                                if self.sanitize:
                                    item_to_write = sanitize_document_recursive(item_to_write)

                                target_container.create_item(body=item_to_write)
                                inserted += 1

                            break  # Success

                        pbar.update(1)

                    continuation_token = page_iterator.continuation_token
                    if not continuation_token:
                        break

            duration = time.time() - start_time
            rate = (inserted + updated) / duration if duration > 0 else 0

            log_success(
                f"Migration completed for container \"{source_container.id}\":\n"
                f"  • Inserted: {format_number(inserted)}\n"
                f"  • Updated:  {format_number(updated)}\n"
                f"  • Skipped:  {format_number(skipped)}\n"
                f"  • Errors:   {format_number(errors)}\n"
                f"  • Duration: {format_time(duration)}\n"
                f"  • Rate:     {format_number(int(rate))} docs/sec"
            )

            return {
                "inserted": inserted,
                "updated": updated,
                "skipped": skipped,
                "errors": errors,
                "duration": duration,
                "rate": rate
            }

        except Exception as e:
            log_error(f"Failed to migrate container \"{source_container.id}\": {str(e)}")
            raise

    def verify_migration(self, source_container, target_container):
        """Verify migration by comparing item counts"""
        try:
            log_info(f"Verifying migration for container \"{source_container.id}\"...")
            count_query = "SELECT VALUE COUNT(1) FROM c"

            source_count = list(source_container.query_items(
                query=count_query,
                enable_cross_partition_query=True
            ))[0]

            target_count = list(target_container.query_items(
                query=count_query,
                enable_cross_partition_query=True
            ))[0]

            if source_count == target_count:
                log_success(f"Verification successful: {format_number(source_count)} items")
                return True, source_count, target_count
            else:
                log_error(
                    f"Verification failed:\n"
                    f"  • Source: {format_number(source_count)}\n"
                    f"  • Target: {format_number(target_count)}"
                )
                return False, source_count, target_count

        except Exception as e:
            log_error(f"Failed to verify migration: {str(e)}")
            return False, 0, 0

# Helper function to remove system fields like _etag, _rid, _self, and _ts
def remove_system_fields(doc):
    """Recursively remove system fields from the document"""
    if isinstance(doc, dict):
        # Remove common system fields that vary during updates
        doc.pop("_etag", None)  # Remove _etag
        doc.pop("_rid", None)   # Remove _rid
        doc.pop("_self", None)  # Remove _self
        doc.pop("_ts", None)    # Remove _ts (timestamp)
        
        # Recursively handle nested structures
        for key in doc:
            if isinstance(doc[key], (dict, list)):
                remove_system_fields(doc[key])  # Recursively remove system fields from nested structures
    return doc