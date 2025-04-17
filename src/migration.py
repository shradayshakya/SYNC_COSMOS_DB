import time
import json
from tqdm import tqdm
from azure.cosmos import exceptions
from .utils import (
    log_info, log_success, log_error, log_stage,
    format_time, format_number
)

class DataMigrator:
    def __init__(self, batch_size=100, max_retries=3):
        self.batch_size = batch_size
        self.max_retries = max_retries

    def _get_partition_key_path(self, container):
        return container.read()["partitionKey"]["paths"][0].strip("/")

    def _get_partition_key_value(self, item, pk_path):
        try:
            keys = pk_path.split("/")
            for key in keys:
                item = item[key]
            return item
        except (KeyError, TypeError):
            return None

    def migrate_container(self, source_container, target_container):
        try:
            start_time = time.time()
            log_stage(f"Starting migration for container \"{source_container.id}\"")

            source_pk_path = self._get_partition_key_path(source_container)
            target_pk_path = self._get_partition_key_path(target_container)

            if source_pk_path != target_pk_path:
                raise ValueError(
                    f"Partition key mismatch in container '{source_container.id}':\n"
                    f"  • Source: {source_pk_path}\n"
                    f"  • Target: {target_pk_path}"
                )

            # Count total documents
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
                            log_error("Skipping item with no 'id'")
                            errors += 1
                            pbar.update(1)
                            continue

                        pk_value = self._get_partition_key_value(item, target_pk_path)

                        if pk_value is None or isinstance(pk_value, (dict, list)):
                            log_error(f"Skipping item {item_id}: invalid/missing partition key '{target_pk_path}'")
                            errors += 1
                            pbar.update(1)
                            continue

                        for attempt in range(1, self.max_retries + 1):
                            try:
                                # Try reading from target
                                try:
                                    target_doc = target_container.read_item(
                                        item=item_id,
                                        partition_key=pk_value
                                    )
                                    skipped += 1
                                except exceptions.CosmosResourceNotFoundError:
                                    target_container.create_item(body=item)
                                    inserted += 1

                                break  # Success
                            except Exception as e:
                                if attempt == self.max_retries:
                                    log_error(f"Failed to process item {item_id}: {str(e)}")
                                    errors += 1
                                else:
                                    time.sleep(0.5 * attempt)

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
        """Verify by comparing document counts."""
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
