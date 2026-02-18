import boto3
import aioboto3
import os
from typing import Dict, Any, List

class DynamoDB:
    def __init__(self, table_name: str = None):
        """
        Initialize DynamoDB connection.
        If table_name is not provided, it looks for DYNAMODB_TABLE env var.
        """
        self.region = os.getenv("AWS_REGION", "us-east-1")
        self.access_key = os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        # Sync client
        self.dynamodb = boto3.resource(
            'dynamodb',
            region_name=self.region,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )
        self.table_name = table_name or os.getenv("DYNAMODB_TABLE")
        
        if self.table_name:
            self.table = self.dynamodb.Table(self.table_name)
        else:
            self.table = None
            print("Warning: Database initialized without a table_name.")

        # Async session
        self.session = aioboto3.Session()
        self.async_resource = None
        self.async_table = None

    async def connect(self):
        """
        Initializes the async boto3 resource and table.
        Must be called at app startup.
        """
        if self.async_resource:
            return

        print(f"DEBUG: Connecting to DynamoDB table {self.table_name} asynchronously...")
        self._resource_cm = self.session.resource(
            "dynamodb",
            region_name=self.region,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
        self.async_resource = await self._resource_cm.__aenter__()
        self.async_table = await self.async_resource.Table(self.table_name)
        print(f"DEBUG: Connected to {self.table_name}.")

    async def close(self):
        """
        Closes the async boto3 resource.
        """
        if self.async_resource:
            print(f"DEBUG: Closing connection to {self.table_name}...")
            await self.async_resource.__aexit__(None, None, None)
            self.async_resource = None
            self.async_table = None

    async def async_read_campaign_metrics(self, range_days: int) -> List[Dict[str, Any]]:
        """
        Asynchronously reads all campaign metrics for a specific time range using GSI query.
        """
        try:
            from boto3.dynamodb.conditions import Key
            
            if self.async_table:
                # Use persistent connection
                response = await self.async_table.query(
                    IndexName='RangeDaysIndex',
                    KeyConditionExpression=Key('range_days').eq(int(range_days))
                )
                items = response.get('Items', [])
                
                while 'LastEvaluatedKey' in response:
                    response = await self.async_table.query(
                        IndexName='RangeDaysIndex',
                        KeyConditionExpression=Key('range_days').eq(int(range_days)),
                        ExclusiveStartKey=response['LastEvaluatedKey']
                    )
                    items.extend(response.get('Items', []))
                
                return items
            else:
                # Fallback to creating new connection (slow)
                async with self.session.resource(
                    'dynamodb',
                    region_name=self.region,
                    aws_access_key_id=self.access_key,
                    aws_secret_access_key=self.secret_key
                ) as dynamodb:
                    table = await dynamodb.Table(self.table_name)
                    
                    response = await table.query(
                        IndexName='RangeDaysIndex',
                        KeyConditionExpression=Key('range_days').eq(int(range_days))
                    )
                    
                    items = response.get('Items', [])
                    
                    while 'LastEvaluatedKey' in response:
                        response = await table.query(
                            IndexName='RangeDaysIndex',
                            KeyConditionExpression=Key('range_days').eq(int(range_days)),
                            ExclusiveStartKey=response['LastEvaluatedKey']
                        )
                        items.extend(response.get('Items', []))
                    
                    return items
        except Exception as e:
            print(f"Async GSI query failed, falling back to sync scan via thread: {e}")
            # Fallback to sync version if something goes wrong with async
            import asyncio
            return await asyncio.to_thread(self.read_campaign_metrics, range_days)

    async def async_read_metrics_by_integrations(self, integration_ids: List[str], range_days: int) -> List[Dict[str, Any]]:
        """
        Fetches metrics for multiple integration IDs in parallel using the IntegrationRangeIndex GSI.
        Provides isolation by only returning data belonging to the specified integrations.
        """
        import asyncio
        from boto3.dynamodb.conditions import Key

        if not integration_ids:
            return []

        async def query_integration(integration_id):
            items = []
            try:
                # We use the same persistent connection pattern as async_read_campaign_metrics
                if self.async_table:
                    response = await self.async_table.query(
                        IndexName='IntegrationRangeIndex',
                        KeyConditionExpression=Key('integration_id').eq(str(integration_id)) & Key('range_days').eq(int(range_days))
                    )
                    items.extend(response.get('Items', []))
                    
                    while 'LastEvaluatedKey' in response:
                        response = await self.async_table.query(
                            IndexName='IntegrationRangeIndex',
                            KeyConditionExpression=Key('integration_id').eq(str(integration_id)) & Key('range_days').eq(int(range_days)),
                            ExclusiveStartKey=response['LastEvaluatedKey']
                        )
                        items.extend(response.get('Items', []))
                else:
                    # Fallback to creating new connection if connect() wasn't called (e.g. in some scripts)
                    async with self.session.resource(
                        'dynamodb',
                        region_name=self.region,
                        aws_access_key_id=self.access_key,
                        aws_secret_access_key=self.secret_key
                    ) as dynamodb:
                        table = await dynamodb.Table(self.table_name)
                        response = await table.query(
                            IndexName='IntegrationRangeIndex',
                            KeyConditionExpression=Key('integration_id').eq(str(integration_id)) & Key('range_days').eq(int(range_days))
                        )
                        items.extend(response.get('Items', []))
                        
                        while 'LastEvaluatedKey' in response:
                            response = await table.query(
                                IndexName='IntegrationRangeIndex',
                                KeyConditionExpression=Key('integration_id').eq(str(integration_id)) & Key('range_days').eq(int(range_days)),
                                ExclusiveStartKey=response['LastEvaluatedKey']
                            )
                            items.extend(response.get('Items', []))
            except Exception as e:
                print(f"Error querying {self.table_name} for integration {integration_id}: {e}")
            return items

        # Trigger parallel queries for each integration_id
        tasks = [query_integration(iid) for iid in integration_ids]
        results = await asyncio.gather(*tasks)
        
        # Flatten the list of lists into a single list of metrics
        flattened_results = [item for sublist in results for item in sublist]
        return flattened_results

    async def async_list_integrations(self, platform: str = None, email: str = None, user_id: str = None) -> List[Dict[str, Any]]:
        """
        Asynchronously lists active integrations.
        Can filter by platform, email, or user_id.
        Always filters is_deleted=False.
        """
        try:
            from boto3.dynamodb.conditions import Attr, Key
            
            # Note: aioboto3 conditions work similarly to boto3
            filter_expr = Attr('is_deleted').eq(False) | Attr('is_deleted').not_exists()
            
            if self.async_table:
                if user_id:
                    # If we have a lot of items, a GSI would be better.
                    # For now scan + filter on user_id
                    filter_expr = filter_expr & Attr('user_id').eq(user_id)
                    response = await self.async_table.scan(FilterExpression=filter_expr)
                elif email:
                    # Use EmailIndex GSI
                    response = await self.async_table.query(
                        IndexName='EmailIndex',
                        KeyConditionExpression=Key('email').eq(email),
                        FilterExpression=filter_expr
                    )
                elif platform:
                    filter_expr = filter_expr & Attr('platform').eq(platform)
                    response = await self.async_table.scan(FilterExpression=filter_expr)
                else:
                    response = await self.async_table.scan(FilterExpression=filter_expr)
                return response.get('Items', [])
            else:
                # Fallback to sync via thread if async setup failed
                import asyncio
                return await asyncio.to_thread(self.list_integrations, platform, email, user_id)
        except Exception as e:
            print(f"Async list integrations failed: {e}")
            import asyncio
            return await asyncio.to_thread(self.list_integrations, platform, email, user_id)

    async def async_get_integration(self, integration_id: str, user_id: str = None) -> Dict[str, Any]:
        """
        Retrieves a specific integration by its UUID.
        Optionally validates that it belongs to the specified user_id.
        """
        try:
            if not self.async_table:
                # Fallback or wait for connect? Assume connected for API.
                return None
                
            from boto3.dynamodb.conditions import Key
            
            # Direct lookup by PK
            response = await self.async_table.get_item(Key={'id': integration_id})
            item = response.get('Item')
            
            if not item or item.get('is_deleted', False):
                return None
                
            # Security check: ensure user owns this integration if user_id provided
            if user_id and item.get('user_id') != user_id:
                print(f"SECURITY ALERT: User {user_id} tried to access integration {integration_id} owned by {item.get('user_id')}")
                return None
                
            return item
        except Exception as e:
            print(f"Error getting integration {integration_id}: {e}")
            return None

    async def async_query(self, **kwargs) -> Dict[str, Any]:
        """
        Asynchronously queries the table.
        """
        try:
            print(f"DEBUG: async_query called on {self.table_name} with keys: {kwargs.keys()}")
            
            if self.async_table:
                return await self.async_table.query(**kwargs)
            else:
                async with self.session.resource(
                    'dynamodb',
                    region_name=self.region,
                    aws_access_key_id=self.access_key,
                    aws_secret_access_key=self.secret_key
                ) as dynamodb:
                    table = await dynamodb.Table(self.table_name)
                    return await table.query(**kwargs)
        except Exception as e:
            print(f"Async query failed on {self.table_name} with kwargs {kwargs.keys()}: {e}")
            import asyncio
            return await asyncio.to_thread(self.table.query, **kwargs)

    async def async_put_item(self, **kwargs) -> Dict[str, Any]:
        """
        Asynchronously puts an item in the table.
        """
        try:
            if self.async_table:
                return await self.async_table.put_item(**kwargs)
            else:
                async with self.session.resource(
                    'dynamodb',
                    region_name=self.region,
                    aws_access_key_id=self.access_key,
                    aws_secret_access_key=self.secret_key
                ) as dynamodb:
                    table = await dynamodb.Table(self.table_name)
                    return await table.put_item(**kwargs)
        except Exception as e:
            print(f"Async put item failed: {e}")
            import asyncio
            return await asyncio.to_thread(self.table.put_item, **kwargs)

    def write_campaign_metrics(self, campaign_id: str, range_days: int, metrics: Dict[str, Any], integration_id: str = None):
        """
        Stores campaign metrics (for Dash). Single item write with retry.
        Includes integration_id for cross-account isolation.
        """
        import datetime
        import time
        
        item = {
            'campaign_id': str(campaign_id),
            'range_days': int(range_days),
            'last_synced': datetime.datetime.utcnow().isoformat(),
            **metrics
        }
        
        if integration_id:
            item['integration_id'] = str(integration_id)
        
        for attempt in range(5):
            try:
                self.table.put_item(Item=item)
                return True
            except Exception as e:
                if 'ProvisionedThroughputExceededException' in str(e):
                    wait_time = (2 ** attempt) * 0.1  # 0.1s, 0.2s, 0.4s, 0.8s, 1.6s
                    print(f"Throughput exceeded, waiting {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"Error storing to {self.table_name}: {str(e)}")
                    return False
        return False

    def batch_write_campaign_metrics(self, campaigns: List[Dict[str, Any]], range_days: int, integration_id: str = None):
        """
        Batch writes campaigns to DynamoDB with retry logic.
        Much faster than individual writes.
        """
        import datetime
        import time
        
        if not campaigns:
            return True
            
        timestamp = datetime.datetime.utcnow().isoformat()
        
        # Prepare items
        items = []
        for campaign in campaigns:
            # We allow campaign to be a dict that might already have campaign_id or it's passed separately
            c_id = campaign.get("campaign_id")
            if c_id:
                # Remove ID from metrics dict to avoid duplication if it's there
                metrics = {k: v for k, v in campaign.items() if k != "campaign_id"}
                
                item = {
                    'campaign_id': str(c_id),
                    'range_days': int(range_days),
                    'last_synced': timestamp,
                    **metrics
                }
                
                if integration_id:
                    item['integration_id'] = str(integration_id)
                elif 'integration_id' in campaign:
                    # Allow it to be passed inside the campaign dict too
                    item['integration_id'] = str(campaign['integration_id'])
                
                items.append(item)
        
        # DynamoDB batch_write_item supports max 25 items per batch
        batch_size = 25
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            
            for attempt in range(5):
                try:
                    with self.table.batch_writer() as writer:
                        for item in batch:
                            writer.put_item(Item=item)
                    break  # Success, move to next batch
                except Exception as e:
                    if 'ProvisionedThroughputExceededException' in str(e):
                        wait_time = (2 ** attempt) * 0.2
                        print(f"Throughput exceeded on batch, waiting {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        print(f"Batch write error: {str(e)}")
                        break
        
        integration_ctx = f" for integration {integration_id}" if integration_id else ""
        print(f"Batch wrote {len(items)} campaigns for {range_days} days{integration_ctx}")
        return True

    def read_campaign_metrics(self, range_days: int) -> List[Dict[str, Any]]:
        """
        Reads all campaign metrics for a specific time range using GSI query.
        Significantly faster than table scan (~10-30ms vs 2-3s).
        """
        try:
            from boto3.dynamodb.conditions import Key
            
            response = self.table.query(
                IndexName='RangeDaysIndex',
                KeyConditionExpression=Key('range_days').eq(int(range_days))
            )
            
            items = response.get('Items', [])
            
            # Handle pagination if more than 1MB of data
            while 'LastEvaluatedKey' in response:
                response = self.table.query(
                    IndexName='RangeDaysIndex',
                    KeyConditionExpression=Key('range_days').eq(int(range_days)),
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response.get('Items', []))
            
            return items
        except Exception as e:
            # Fallback to scan if GSI doesn't exist yet
            print(f"GSI query failed, falling back to scan: {e}")
            from boto3.dynamodb.conditions import Attr
            response = self.table.scan(
                FilterExpression=Attr('range_days').eq(int(range_days))
            )
            return response.get('Items', [])
    def save_integration(self, platform: str, account_id: str, email: str, access_token: str, 
                        account_name: str = None, status: str = "Active", last_synced: str = None,
                        needs_reauth: bool = False, last_token_refresh_at: str = None, access_token_expires_at: str = None,
                        user_id: str = None):
        """
        Stores account integration details. 
        Uses UUID as PK. Checks for existing integration by platform+account_id to update or resurrect.
        Returns the integration UUID (id).
        """
        try:
            import datetime
            import uuid
            
            timestamp = datetime.datetime.utcnow().isoformat()
            
            # 1. Check if integration exists (via GSI) to get its UUID
            existing_items = self.query(
                IndexName='PlatformAccountIndex',
                KeyConditionExpression="platform = :p AND account_id = :a",
                ExpressionAttributeValues={":p": platform, ":a": str(account_id)}
            ).get('Items', [])
            
            if existing_items:
                # Update existing
                item = existing_items[0]
                # Ensure we resurrect if it was deleted
                item['is_deleted'] = False
            else:
                # Create new
                item = {
                    'id': str(uuid.uuid4()),
                    'created_at': timestamp,
                    'is_deleted': False
                }

            # Update fields
            item.update({
                'platform': platform,
                'account_id': str(account_id),
                'email': email,
                'access_token': access_token,
                'account_name': account_name or account_id,
                'status': status,
                'last_synced': last_synced or timestamp,
                'needs_reauth': needs_reauth,
                'last_token_refresh_at': last_token_refresh_at,
                'access_token_expires_at': access_token_expires_at,
                'updated_at': timestamp
            })
            
            if user_id:
                item['user_id'] = user_id

            # Remove None values
            item = {k: v for k, v in item.items() if v is not None}
            
            self.table.put_item(Item=item)
            return item['id']
        except Exception as e:
            print(f"Error saving integration: {str(e)}")
            return None

    def update_integration_status(self, platform: str, account_id: str, needs_reauth: bool = None, error_message: str = None):
        """
        Updates just the status/reauth flags of an integration.
        Requires lookup of UUID first.
        """
        try:
            if not self.table: return False
            
            # Lookup UUID
            # We could pass UUID if we had it, but usually we have platform+acc_id from callbacks
            existing_items = self.query(
                IndexName='PlatformAccountIndex',
                KeyConditionExpression="platform = :p AND account_id = :a",
                ExpressionAttributeValues={":p": platform, ":a": str(account_id)}
            ).get('Items', [])
            
            if not existing_items:
                print(f"Cannot update status: Integration not found for {platform}:{account_id}")
                return False
                
            integration_id = existing_items[0]['id']
            
            key = {'id': integration_id}
            
            update_expr = "SET "
            expr_attrs = {}
            
            updates = []
            if needs_reauth is not None:
                updates.append("needs_reauth = :nr")
                expr_attrs[":nr"] = needs_reauth
                
            if error_message:
                updates.append("last_error = :le")
                expr_attrs[":le"] = error_message
                
            if not updates:
                return True
                
            update_expr += ", ".join(updates)
            
            self.table.update_item(
                Key=key,
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_attrs
            )
            return True
        except Exception as e:
            print(f"Error updating integration status: {e}")
            return False

    def delete_integration(self, platform: str, account_id: str):
        """
        Soft deletes an integration (sets is_deleted=True).
        """
        try:
            # Lookup UUID
            existing_items = self.query(
                IndexName='PlatformAccountIndex',
                KeyConditionExpression="platform = :p AND account_id = :a",
                ExpressionAttributeValues={":p": platform, ":a": str(account_id)}
            ).get('Items', [])
            
            if not existing_items:
                return False # Already gone or never existed
                
            integration_id = existing_items[0]['id']

            self.table.update_item(
                Key={'id': integration_id},
                UpdateExpression="SET is_deleted = :d, status = :s",
                ExpressionAttributeValues={":d": True, ":s": "Inactive"}
            )
            return True
        except Exception as e:
            print(f"Error deleting integration: {str(e)}")
            return False

    async def async_delete_integration(self, platform: str, account_id: str):
        """
        Asynchronously soft deletes an integration.
        """
        try:
             # We need to query first to find the ID.
             # This assumes we have async query capability.
             # For simplicity, we can reuse the sync logic wrapped in a thread if async query is complex,
             # but we have async_query method.
            if self.async_table:
                # 1. Query GSI
                resp = await self.async_table.query(
                    IndexName='PlatformAccountIndex',
                    KeyConditionExpression="platform = :p AND account_id = :a",
                    ExpressionAttributeValues={":p": platform, ":a": str(account_id)}
                )
                items = resp.get('Items', [])
                if not items: return False
                
                integration_id = items[0]['id']
                
                # 2. Update
                await self.async_table.update_item(
                    Key={'id': integration_id},
                    UpdateExpression="SET is_deleted = :d, status = :s",
                    ExpressionAttributeValues={":d": True, ":s": "Inactive"}
                )
                return True
            else:
                 # Fallback to sync
                import asyncio
                return await asyncio.to_thread(self.delete_integration, platform, account_id)
        except Exception as e:
            print(f"Error asynchronously deleting integration: {str(e)}")
            return False

    async def async_delete_by_id(self, integration_id: str):
        """
        Asynchronously soft deletes an integration by its ID.
        """
        try:
            if self.async_table:
                await self.async_table.update_item(
                    Key={'id': integration_id},
                    UpdateExpression="SET is_deleted = :d, status = :s",
                    ExpressionAttributeValues={":d": True, ":s": "Inactive"}
                )
                return True
            else:
                # Fallback to sync
                def sync_delete():
                    return self.table.update_item(
                        Key={'id': integration_id},
                        UpdateExpression="SET is_deleted = :d, status = :s",
                        ExpressionAttributeValues={":d": True, ":s": "Inactive"}
                    )
                import asyncio
                await asyncio.to_thread(sync_delete)
                return True
        except Exception as e:
            print(f"Error deleting integration by ID {integration_id}: {e}")
            return False

    def list_integrations(self, platform: str = None, email: str = None, user_id: str = None) -> List[Dict[str, Any]]:
        """
        Lists active integrations.
        Can filter by platform, email, or user_id.
        Always filters is_deleted=False.
        """
        try:
            from boto3.dynamodb.conditions import Attr, Key
            
            # Base filter: not deleted
            filter_expr = Attr('is_deleted').eq(False) | Attr('is_deleted').not_exists()
            
            if user_id:
                # Scan with filter (no GSI yet)
                filter_expr = filter_expr & Attr('user_id').eq(user_id)
                response = self.table.scan(FilterExpression=filter_expr)
            elif email:
                # Use EmailIndex GSI
                response = self.table.query(
                    IndexName='EmailIndex',
                    KeyConditionExpression=Key('email').eq(email),
                    FilterExpression=filter_expr
                )
            elif platform:
                # Scan or PlatformIndex (if we had one just for platform, but we have platform+acc)
                # We can use PlatformAccountIndex but we need account_id for range.
                # So we just scan with filter
                filter_expr = filter_expr & Attr('platform').eq(platform)
                response = self.table.scan(FilterExpression=filter_expr)
            else:
                response = self.table.scan(FilterExpression=filter_expr)
                
            return response.get('Items', [])
        except Exception as e:
            print(f"Error listing integrations: {str(e)}")
            return []
            
    def query(self, **kwargs):
        """Sync query helper"""
        return self.table.query(**kwargs)


    def create_table(self, pk: str = 'campaign_id', sk: str = 'range_days', sk_type: str = 'N'):
        """
        Creates the table if it doesn't exist.
        Default PK is campaign_id (S), SK is range_days (N).
        """
        try:
            existing_tables = [t.name for t in self.dynamodb.tables.all()]
            if self.table_name in existing_tables:
                return True
                
            print(f"Creating table {self.table_name}...")
            
            # Simple wrapper handling specific schema overrides based on table name would be cleaner,
            # but for now we follow the existing pattern or passed params.
            # However, Integrations now needs a DIFFERENT schema (pk=id only) than the metrics tables.
            
            key_schema = [{'AttributeName': pk, 'KeyType': 'HASH'}]
            attr_defs = [{'AttributeName': pk, 'AttributeType': 'S'}]
            
            if sk:
                key_schema.append({'AttributeName': sk, 'KeyType': 'RANGE'})
                attr_defs.append({'AttributeName': sk, 'AttributeType': sk_type})
                
            table = self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=key_schema,
                AttributeDefinitions=attr_defs,
                ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            )
            table.meta.client.get_waiter('table_exists').wait(TableName=self.table_name)
            self.table = self.dynamodb.Table(self.table_name)
            return True
        except Exception as e:
            print(f"Error creating table: {str(e)}")
            return False

    def create_integrations_gsis(self):
        """
        Creates EmailIndex and PlatformAccountIndex for Integrations table.
        """
        try:
            print(f"Creating GSIs for {self.table_name}...")
            
            # Helper to check existing GSIs
            table_desc = self.table.meta.client.describe_table(TableName=self.table_name)
            existing_gsis = [g['IndexName'] for g in table_desc.get('Table', {}).get('GlobalSecondaryIndexes', [])]
            
            updates = []
            
            # 1. EmailIndex (PK=email)
            if 'EmailIndex' not in existing_gsis:
                updates.append({
                    'Create': {
                        'IndexName': 'EmailIndex',
                        'KeySchema': [{'AttributeName': 'email', 'KeyType': 'HASH'}],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                    }
                })
                
            # 2. PlatformAccountIndex (PK=platform, SK=account_id)
            if 'PlatformAccountIndex' not in existing_gsis:
                updates.append({
                    'Create': {
                        'IndexName': 'PlatformAccountIndex',
                        'KeySchema': [
                            {'AttributeName': 'platform', 'KeyType': 'HASH'},
                            {'AttributeName': 'account_id', 'KeyType': 'RANGE'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                    }
                })
                
            if not updates:
                print("GSIs already exist.")
                return True
                
            # Add AttributeDefinitions for GSI keys if not present
            # We can't easily know which ones are already there from 'attr_defs', so we just try to provide them. 
            # Actually, standard practice is to just include them.
            
            attr_defs = [
                {'AttributeName': 'email', 'AttributeType': 'S'},
                {'AttributeName': 'platform', 'AttributeType': 'S'},
                {'AttributeName': 'account_id', 'AttributeType': 'S'}
            ]
            
            # Apply updates one by one to avoid LimitExceededException
            for update in updates:
                 gsi_name = update['Create']['IndexName']
                 print(f"Creating GSI: {gsi_name}...")
                 
                 self.table.update(
                    AttributeDefinitions=attr_defs,
                    GlobalSecondaryIndexUpdates=[update]
                 )
                 
                 # Wait for table to be ACTIVE again before next update
                 print(f"Waiting for {gsi_name} to be ACTIVE...")
                 self.table.meta.client.get_waiter('table_exists').wait(TableName=self.table_name)
                 
                 # The standard waiter just waits for table to exist/be active. 
                 # However, GSI creation takes time. We might need a loop checking describe_table.
                 import time
                 while True:
                     desc = self.table.meta.client.describe_table(TableName=self.table_name)
                     status = desc['Table']['TableStatus']
                     
                     # Check specific GSI status
                     gsi_status = 'UNKNOWN'
                     for g in desc['Table'].get('GlobalSecondaryIndexes', []):
                         if g['IndexName'] == gsi_name:
                             gsi_status = g['IndexStatus']
                             break
                             
                     print(f"  Table Status: {status}, GSI {gsi_name}: {gsi_status}")
                     
                     if status == 'ACTIVE' and gsi_status == 'ACTIVE':
                         print(f"✅ GSI {gsi_name} is active.")
                         break
                         
                     if status == 'UPDATING' or gsi_status == 'CREATING':
                         time.sleep(5)
                     else:
                         # Start next one
                         time.sleep(2)

            print("All GSIs created successfully.")
            return True
        except Exception as e:
            print(f"Error creating GSIs: {e}")
            return False

    def create_users_gsis(self):
        """
        Creates EmailIndex for Users table (since PK is now id).
        """
        try:
            print(f"Creating GSIs for {self.table_name}...")
            
            table_desc = self.table.meta.client.describe_table(TableName=self.table_name)
            existing_gsis = [g['IndexName'] for g in table_desc.get('Table', {}).get('GlobalSecondaryIndexes', [])]
            
            updates = []
            
            # EmailIndex (PK=email)
            if 'EmailIndex' not in existing_gsis:
                updates.append({
                    'Create': {
                        'IndexName': 'EmailIndex',
                        'KeySchema': [{'AttributeName': 'email', 'KeyType': 'HASH'}],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                    }
                })
                
            if not updates:
                print("GSIs already exist.")
                return True
                
            attr_defs = [{'AttributeName': 'email', 'AttributeType': 'S'}]
            
            self.table.update(
                AttributeDefinitions=attr_defs,
                GlobalSecondaryIndexUpdates=updates
            )
            print("Users GSI creation initiated.")
            return True
        except Exception as e:
            print(f"Error creating Users GSIs: {e}")
            return False

    def create_range_days_gsi(self):
        """
        Creates a GSI on range_days for efficient querying by time range.
        This replaces expensive table scans with fast queries.
        """
        try:
            # Check if GSI already exists
            table_desc = self.table.meta.client.describe_table(TableName=self.table_name)
            existing_gsis = table_desc.get('Table', {}).get('GlobalSecondaryIndexes', [])
            
            if any(gsi['IndexName'] == 'RangeDaysIndex' for gsi in existing_gsis):
                print(f"✅ GSI 'RangeDaysIndex' already exists on {self.table_name}")
                return True
                
            # Create GSI
            print(f"Creating GSI 'RangeDaysIndex' on {self.table_name}...")
            self.table.update(
                AttributeDefinitions=[
                    {'AttributeName': 'range_days', 'AttributeType': 'N'},
                    {'AttributeName': 'campaign_id', 'AttributeType': 'S'}
                ],
                GlobalSecondaryIndexUpdates=[{
                    'Create': {
                        'IndexName': 'RangeDaysIndex',
                        'KeySchema': [
                            {'AttributeName': 'range_days', 'KeyType': 'HASH'},
                            {'AttributeName': 'campaign_id', 'KeyType': 'RANGE'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 15,
                            'WriteCapacityUnits': 15
                        }
                    }
                }]
            )
            
            print(f"✅ GSI 'RangeDaysIndex' created. Building in background (5-10 min)...")
            return True
        except Exception as e:
            print(f"Error creating GSI: {e}")
            return False

    def update_throughput(self, rcu: int = 15, wcu: int = 15):
        """
        Updates the provisioned throughput for the table.
        """
        try:
            self.table.update(
                ProvisionedThroughput={
                    'ReadCapacityUnits': rcu,
                    'WriteCapacityUnits': wcu
                }
            )
            print(f"Updated {self.table_name} throughput to RCU={rcu}, WCU={wcu}")
            return True
        except Exception as e:
            print(f"Error updating throughput: {str(e)}")
            return False

    async def async_get_user(self, email: str) -> Dict[str, Any]:
        """
        Retrieves a user by email using GSI query (since PK is now uuid).
        """
        try:
            from boto3.dynamodb.conditions import Key
            print(f"DEBUG: async_get_user called for {email} on {self.table_name}")
            
            # Use GSI 'EmailIndex'
            response = await self.async_query(
                IndexName='EmailIndex',
                KeyConditionExpression=Key('email').eq(email)
            )
            items = response.get('Items', [])
            return items[0] if items else None
        except Exception as e:
            print(f"Error getting user by email ({email}) on {self.table_name}: {e}")
            return None

    async def async_get_user_by_id(self, user_id: str) -> Dict[str, Any]:
        """
        Retrieves a user by their UUID primary key.
        """
        try:
            from boto3.dynamodb.conditions import Key
            # Direct get item by PK
            response = await self.async_query(KeyConditionExpression=Key('id').eq(user_id))
            items = response.get('Items', [])
            return items[0] if items else None
        except Exception as e:
            print(f"Error getting user by id: {e}")
            return None

    async def async_create_user(self, email: str, password_hash: str) -> str:
        """
        Creates a new user with UUID. Returns user_id if successful, None if user exists or error.
        """
        try:
            import datetime
            import uuid
            
            # Check if user already exists
            existing_user = await self.async_get_user(email)
            if existing_user:
                return None
                
            timestamp = datetime.datetime.utcnow().isoformat()
            user_id = str(uuid.uuid4())
            
            item = {
                'id': user_id,
                'email': email,
                'password_hash': password_hash,
                'created_at': timestamp,
                'preferences': {}
            }
            
            await self.async_put_item(Item=item)
            return user_id
        except Exception as e:
            print(f"Error creating user: {e}")
            return None

    async def async_update_user_preferences(self, email: str, preferences: Dict[str, Any]) -> Dict[str, Any]:
        """
        Updates user preferences. Returns the updated preferences dict if successful, None otherwise.
        """
        try:
            user = await self.async_get_user(email)
            if not user:
                return None
                
            current_prefs = user.get('preferences', {}) or {}
            current_prefs.update(preferences)
            user['preferences'] = current_prefs
            
            # Put back the entire item
            await self.async_put_item(Item=user)
            return current_prefs
        except Exception as e:
            print(f"Error updating user preferences: {e}")
            return None
