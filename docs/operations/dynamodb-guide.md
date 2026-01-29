# DynamoDB Nameservice Guide

## Overview

Fluree supports Amazon DynamoDB as a nameservice backend for storing ledger metadata. The DynamoDB nameservice provides:

- **Atomic conditional updates**: No contention between transactors and indexers
- **Strong consistency reads**: Always see the latest data
- **High availability**: DynamoDB's built-in redundancy and durability
- **Scalability**: Handles high throughput without coordination

### Why DynamoDB for Nameservice?

The nameservice stores metadata about ledgers: the latest commit address, commit t-value, index address, and index t-value. In high-throughput scenarios, transactors and indexers may update this metadata concurrently, leading to contention with file-based or S3 nameservices.

DynamoDB solves this because:

1. **Separate attributes**: Commit data and index data are stored as separate DynamoDB attributes
2. **Conditional updates**: Each update only proceeds if the new t-value is greater than the existing one
3. **No read-modify-write cycles**: Updates are atomic, eliminating race conditions

## Table Setup

### AWS CLI

Create the DynamoDB table with the following command:

```bash
aws dynamodb create-table \
  --table-name fluree-nameservice \
  --attribute-definitions AttributeName=ledger_alias,AttributeType=S \
  --key-schema AttributeName=ledger_alias,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST
```

### CloudFormation

```yaml
Resources:
  FlureeNameserviceTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: fluree-nameservice
      BillingMode: PAY_PER_REQUEST
      AttributeDefinitions:
        - AttributeName: ledger_alias
          AttributeType: S
      KeySchema:
        - AttributeName: ledger_alias
          KeyType: HASH
      PointInTimeRecoverySpecification:
        PointInTimeRecoveryEnabled: true
      Tags:
        - Key: Application
          Value: Fluree
```

### Terraform

```hcl
resource "aws_dynamodb_table" "fluree_nameservice" {
  name         = "fluree-nameservice"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "ledger_alias"

  attribute {
    name = "ledger_alias"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = {
    Application = "Fluree"
  }
}
```

## Table Schema

### Primary Key

| Attribute | Type | Description |
|-----------|------|-------------|
| `ledger_alias` | String (PK) | Ledger identifier, e.g., `my-ledger:main` |

### Core Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `ledger_name` | String | Ledger name without branch (e.g., `mydb`) |
| `branch` | String | Branch name (e.g., `main`) |
| `commit_address` | String | Latest commit storage address |
| `commit_t` | Number | Transaction time of latest commit |
| `index_address` | String | Latest index storage address |
| `index_t` | Number | Transaction time of latest index |
| `default_context_address` | String | Default JSON-LD context address |
| `status` | String | Ledger status (`ready` or `retracted`) |
| `updated_at` | Number | Last update timestamp (Unix epoch seconds) |

### V2 Extension Attributes

These attributes support the four-concerns model for status and config management:

| Attribute | Type | Description |
|-----------|------|-------------|
| `status_v` | Number | Status watermark (monotonically increasing). Defaults to 1 if missing. |
| `status_meta` | Map | Extensible status metadata (queue_depth, locks, progress, etc.) |
| `config_v` | Number | Config watermark (monotonically increasing). Defaults to 0 if missing. |
| `config_meta` | Map | Extensible config metadata (index_threshold, replication, etc.) |

### How Updates Work

**Commit updates** (transactor):
```
UpdateExpression: SET commit_address = :addr, commit_t = :t, ...
ConditionExpression: attribute_not_exists(commit_t) OR commit_t < :t
```

**Index updates** (indexer):
```
UpdateExpression: SET index_address = :addr, index_t = :t, ...
ConditionExpression: attribute_not_exists(index_t) OR index_t < :t
```

Since commit and index updates modify different attributes, they never conflict!

**Retract** (with status_v bump):
```
UpdateExpression: SET status = :retracted, updated_at = :now ADD status_v :one
```

## Configuration

### JSON-LD Connection Configuration

```json
{
  "@context": {
    "@vocab": "https://ns.flur.ee/system#"
  },
  "@graph": [
    {
      "@id": "s3Storage",
      "@type": "Storage",
      "s3Bucket": "fluree-production-data",
      "s3Endpoint": "https://s3.us-east-1.amazonaws.com",
      "s3Prefix": "ledgers",
      "addressIdentifier": "prod-s3"
    },
    {
      "@id": "dynamodbNs",
      "@type": "Publisher",
      "dynamodbTable": "fluree-nameservice",
      "dynamodbRegion": "us-east-1"
    },
    {
      "@id": "connection",
      "@type": "Connection",
      "parallelism": 4,
      "cacheMaxMb": 1000,
      "commitStorage": {"@id": "s3Storage"},
      "indexStorage": {"@id": "s3Storage"},
      "primaryPublisher": {"@id": "dynamodbNs"}
    }
  ]
}
```

### Configuration Options

| Field | Required | Description | Default |
|-------|----------|-------------|---------|
| `dynamodbTable` | Yes | DynamoDB table name | - |
| `dynamodbRegion` | No | AWS region | `us-east-1` |
| `dynamodbEndpoint` | No | Custom endpoint URL (for LocalStack) | AWS default |
| `dynamodbTimeoutMs` | No | Request timeout in milliseconds | `5000` |

## AWS Credentials

### Authentication Methods

The DynamoDB nameservice uses the standard AWS SDK credential chain:

1. **Environment Variables**
   ```bash
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_REGION=us-east-1
   ```

2. **AWS Credentials File** (`~/.aws/credentials`)
   ```ini
   [default]
   aws_access_key_id = your_access_key
   aws_secret_access_key = your_secret_key
   region = us-east-1
   ```

3. **IAM Roles** (when running on EC2/ECS/Lambda)
   - Automatically uses instance/task role credentials

4. **Session Tokens** (for temporary credentials)
   ```bash
   export AWS_SESSION_TOKEN=your_session_token
   ```

### Required IAM Permissions

Full permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:DeleteItem",
        "dynamodb:Scan"
      ],
      "Resource": "arn:aws:dynamodb:*:*:table/fluree-nameservice"
    }
  ]
}
```

Minimal permissions (if not using `all_records`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:UpdateItem",
        "dynamodb:PutItem"
      ],
      "Resource": "arn:aws:dynamodb:*:*:table/fluree-nameservice"
    }
  ]
}
```

## Local Development

### Using LocalStack

1. **Start LocalStack**
   ```bash
   docker run -d --name localstack \
     -p 4566:4566 \
     -e SERVICES=dynamodb \
     localstack/localstack
   ```

2. **Create Test Table**
   ```bash
   AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
   aws --endpoint-url=http://localhost:4566 dynamodb create-table \
     --table-name fluree-nameservice \
     --attribute-definitions AttributeName=ledger_alias,AttributeType=S \
     --key-schema AttributeName=ledger_alias,KeyType=HASH \
     --billing-mode PAY_PER_REQUEST
   ```

3. **Configure Fluree**
   ```json
   {
     "@id": "dynamodbNs",
     "@type": "Publisher",
     "dynamodbTable": "fluree-nameservice",
     "dynamodbEndpoint": "http://localhost:4566",
     "dynamodbRegion": "us-east-1"
   }
   ```

4. **Set Environment Variables**
   ```bash
   export AWS_ACCESS_KEY_ID=test
   export AWS_SECRET_ACCESS_KEY=test
   ```

### Using DynamoDB Local

1. **Start DynamoDB Local**
   ```bash
   docker run -d --name dynamodb-local \
     -p 8000:8000 \
     amazon/dynamodb-local
   ```

2. **Create Test Table**
   ```bash
   AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
   aws --endpoint-url=http://localhost:8000 dynamodb create-table \
     --table-name fluree-nameservice \
     --attribute-definitions AttributeName=ledger_alias,AttributeType=S \
     --key-schema AttributeName=ledger_alias,KeyType=HASH \
     --billing-mode PAY_PER_REQUEST
   ```

## Production Considerations

### Performance

- DynamoDB provides single-digit millisecond latency
- Use on-demand (PAY_PER_REQUEST) billing for variable workloads
- Consider provisioned capacity for predictable high-throughput scenarios
- Enable DynamoDB Accelerator (DAX) if sub-millisecond reads are needed

### Security

- Use IAM roles instead of access keys when possible
- Enable encryption at rest (default for new tables)
- Use VPC endpoints for private DynamoDB access
- Enable CloudTrail for audit logging

### Monitoring

Set up CloudWatch alarms for:

- `ConditionalCheckFailedRequests` - indicates contention (usually normal)
- `ThrottledRequests` - capacity issues
- `SystemErrors` - service issues
- `SuccessfulRequestLatency` - track latency

### Backup and Recovery

```bash
# Enable Point-in-Time Recovery
aws dynamodb update-continuous-backups \
  --table-name fluree-nameservice \
  --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true

# Create on-demand backup
aws dynamodb create-backup \
  --table-name fluree-nameservice \
  --backup-name fluree-ns-backup-$(date +%Y%m%d)
```

### Cost Optimization

- On-demand pricing is cost-effective for variable workloads
- Table data is small (one item per ledger), so costs are minimal
- Typical costs: $1-10/month for small deployments

## Troubleshooting

### Authentication Failures

**Symptoms**: Access denied, credential errors

**Solutions**:
- Verify AWS credentials are configured
- Check IAM permissions for the table
- Test with AWS CLI:
  ```bash
  aws dynamodb describe-table --table-name fluree-nameservice
  ```

### Table Not Found

**Symptoms**: ResourceNotFoundException

**Solutions**:
- Verify table name is correct
- Check table is in the correct region
- Ensure table has finished creating

### Timeout Errors

**Symptoms**: Request timeout

**Solutions**:
- Increase `dynamodbTimeoutMs` configuration
- Check network connectivity to DynamoDB
- Verify endpoint URL is correct (especially for LocalStack)

### Conditional Check Failures

**Symptoms**: High rate of ConditionalCheckFailedException in logs

**Note**: This is usually normal and indicates the system is working correctly. The conditional check prevents overwriting newer data with older data.

## Related Documentation

- [Storage Modes](storage.md) - Overview of all storage options
- [Configuration](configuration.md) - Full configuration reference
- [Nameservice Schema v2 Design](../design/nameservice-schema-v2.md) - Schema design details
