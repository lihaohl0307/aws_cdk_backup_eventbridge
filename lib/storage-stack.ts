import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Bucket, BlockPublicAccess, BucketEncryption } from 'aws-cdk-lib/aws-s3';
import { AttributeType, BillingMode, Table, ProjectionType, GlobalSecondaryIndexProps } from 'aws-cdk-lib/aws-dynamodb';
import { RemovalPolicy } from 'aws-cdk-lib';

export class StorageStack extends cdk.Stack {
  public readonly bucketSrc: Bucket;
  public readonly bucketDst: Bucket;
  public readonly table: Table;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    this.bucketSrc = new Bucket(this, 'BucketSrc', {
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      encryption: BucketEncryption.S3_MANAGED,
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      eventBridgeEnabled: true, // send S3 events to EventBridge instead of direct bucket notifications
    });

    this.bucketDst = new Bucket(this, 'BucketDst', {
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      encryption: BucketEncryption.S3_MANAGED,
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    this.table = new Table(this, 'BackupMapping', {
      partitionKey: { name: 'srcKey', type: AttributeType.STRING },
      sortKey: { name: 'createdAt', type: AttributeType.NUMBER },
      billingMode: BillingMode.PAY_PER_REQUEST,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // GSI1: status + disownedAt (for Cleaner)
    const gsiStatusAge: GlobalSecondaryIndexProps = {
      indexName: 'GSI_ByStatusAge',
      partitionKey: { name: 'status', type: AttributeType.STRING },
      sortKey: { name: 'disownedAt', type: AttributeType.NUMBER },
      projectionType: ProjectionType.ALL,
    };
    this.table.addGlobalSecondaryIndex(gsiStatusAge);

    // GSI2: srcKey + statusCreatedAt (for Replicator)
    const gsiSrcStatus: GlobalSecondaryIndexProps = {
      indexName: 'GSI_BySrcStatus',
      partitionKey: { name: 'srcKey', type: AttributeType.STRING },
      sortKey: { name: 'statusCreatedAt', type: AttributeType.STRING },
      projectionType: ProjectionType.ALL,
    };
    this.table.addGlobalSecondaryIndex(gsiSrcStatus);

    new cdk.CfnOutput(this, 'BucketSrcName', { value: this.bucketSrc.bucketName });
    new cdk.CfnOutput(this, 'BucketDstName', { value: this.bucketDst.bucketName });
    new cdk.CfnOutput(this, 'TableName', { value: this.table.tableName });
  }
}
