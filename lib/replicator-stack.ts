import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { Table } from 'aws-cdk-lib/aws-dynamodb';
import { Code, Function as LambdaFn, Runtime } from 'aws-cdk-lib/aws-lambda';
import { Duration } from 'aws-cdk-lib';
import { Rule, EventPattern, Schedule } from 'aws-cdk-lib/aws-events';
import { LambdaFunction } from 'aws-cdk-lib/aws-events-targets';

interface Props extends cdk.StackProps {
  table: Table;
  bucketSrc: Bucket;
  bucketDst: Bucket;
}

export class ReplicatorStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id, props);

    const fn = new LambdaFn(this, 'ReplicatorFn', {
      runtime: Runtime.PYTHON_3_11,
      handler: 'replicator.handler',
      code: Code.fromAsset('lambda'),
      timeout: Duration.seconds(30),
      environment: {
        TABLE_NAME: props.table.tableName,
        BUCKET_SRC: props.bucketSrc.bucketName,
        BUCKET_DST: props.bucketDst.bucketName,
        GSI_SRC_STATUS: 'GSI_BySrcStatus',
        MAX_ACTIVE: '3',
      },
    });

    props.table.grantReadWriteData(fn);
    props.bucketSrc.grantRead(fn);
    props.bucketDst.grantReadWrite(fn);

    // EventBridge rule for S3 events
    const pattern: EventPattern = {
      source: ['aws.s3'],
      // S3 → EventBridge uses these detail types:
      detailType: ['Object Created', 'Object Deleted'],
      detail: {
        bucket: { name: [props.bucketSrc.bucketName] },
        // optional: filter specific operations
        // object: { key: [{ prefix: '' }] }
      },
    };

    const rule = new Rule(this, 'S3EventsToReplicator', { eventPattern: pattern });
    rule.addTarget(new LambdaFunction(fn));
  }
}
