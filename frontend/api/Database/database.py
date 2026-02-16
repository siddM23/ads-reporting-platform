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

    async def async_list_integrations(self, platform: str = None) -> List[Dict[str, Any]]:
        """
        Asynchronously lists all integrations.
        """
        try:
            if self.async_table:
                if platform:
                    response = await self.async_table.query(
                        KeyConditionExpression="platform = :p",
                        ExpressionAttributeValues={":p": platform}
                    )
                else:
                    response = await self.async_table.scan()
                return response.get('Items', [])
            else:
                async with self.session.resource(
                    'dynamodb',
                    region_name=self.region,
                    aws_access_key_id=self.access_key,
                    aws_secret_access_key=self.secret_key
                ) as dynamodb:
                    table = await dynamodb.Table(self.table_name)
                    
                    if platform:
                        response = await table.query(
                            KeyConditionExpression="platform = :p",
                            ExpressionAttributeValues={":p": platform}
                        )
                    else:
                        response = await table.scan()
                    return response.get('Items', [])
        except Exception as e:
            print(f"Async list integrations failed: {e}")
            import asyncio
            return await asyncio.to_thread(self.list_integrations, platform)

    async def async_query(self, **kwargs) -> Dict[str, Any]:
        """
        Asynchronously queries the table.
        """
        try:
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
            print(f"Async query failed: {e}")
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

    def write_campaign_metrics(self, campaign_id: str, range_days: int, metrics: Dict[str, Any]):
        """
        Stores campaign metrics (for Dash). Single item write with retry.
        """
        import datetime
        import time
        
        item = {
            'campaign_id': str(campaign_id),
            'range_days': int(range_days),
            'last_synced': datetime.datetime.utcnow().isoformat(),
            **metrics
        }
        
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

    def batch_write_campaign_metrics(self, campaigns: List[Dict[str, Any]], range_days: int):
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
            campaign_id = campaign.get("campaign_id")
            if campaign_id:
                metrics = {k: v for k, v in campaign.items() if k != "campaign_id"}
                items.append({
                    'campaign_id': str(campaign_id),
                    'range_days': int(range_days),
                    'last_synced': timestamp,
                    **metrics
                })
        
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
        
        print(f"Batch wrote {len(items)} campaigns for {range_days} days")
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
    def save_integration(self, platform: str, account_id: str, email: str, access_token: str, account_name: str = None, status: str = "Active", last_synced: str = None):
        """
        Stores account integration details.
        """
        try:
            import datetime
            item = {
                'platform': platform,
                'account_id': str(account_id),
                'email': email,
                'access_token': access_token,
                'account_name': account_name or account_id,
                'status': status,
                'last_synced': last_synced or datetime.datetime.utcnow().isoformat()
            }
            self.table.put_item(Item=item)
            return True
        except Exception as e:
            print(f"Error saving integration: {str(e)}")
            return False

    def delete_integration(self, platform: str, account_id: str):
        """
        Deletes an integration from the table.
        """
        try:
            self.table.delete_item(
                Key={
                    'platform': platform,
                    'account_id': str(account_id)
                }
            )
            return True
        except Exception as e:
            print(f"Error deleting integration: {str(e)}")
            return False

    async def async_delete_integration(self, platform: str, account_id: str):
        """
        Asynchronously deletes an integration from the table.
        """
        try:
            if self.async_table:
                await self.async_table.delete_item(
                    Key={
                        'platform': platform,
                        'account_id': str(account_id)
                    }
                )
                return True
            else:
                async with self.session.resource(
                    'dynamodb',
                    region_name=self.region,
                    aws_access_key_id=self.access_key,
                    aws_secret_access_key=self.secret_key
                ) as dynamodb:
                    table = await dynamodb.Table(self.table_name)
                    await table.delete_item(
                        Key={
                            'platform': platform,
                            'account_id': str(account_id)
                        }
                    )
                    return True
        except Exception as e:
            print(f"Error asynchronously deleting integration: {str(e)}")
            return False

    def list_integrations(self, platform: str = None) -> List[Dict[str, Any]]:
        """
        Lists all integrations, optionally filtered by platform.
        """
        try:
            if platform:
                # Assuming 'platform' is the partition key for an Integrations table
                # Or we can just scan if the table is small
                response = self.table.query(
                    KeyConditionExpression="platform = :p",
                    ExpressionAttributeValues={":p": platform}
                )
            else:
                response = self.table.scan()
            return response.get('Items', [])
        except Exception as e:
            print(f"Error listing integrations: {str(e)}")
            return []

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
            table = self.dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {'AttributeName': pk, 'KeyType': 'HASH'},
                    {'AttributeName': sk, 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': pk, 'AttributeType': 'S'},
                    {'AttributeName': sk, 'AttributeType': sk_type}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 15, 'WriteCapacityUnits': 15}
            )
            table.meta.client.get_waiter('table_exists').wait(TableName=self.table_name)
            self.table = self.dynamodb.Table(self.table_name)
            return True
        except Exception as e:
            print(f"Error creating table: {str(e)}")
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
