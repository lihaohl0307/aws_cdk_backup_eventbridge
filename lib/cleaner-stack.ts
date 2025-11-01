import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Bucket } from 'aws-cdk-lib/aws-s3';
import { Table } from 'aws-cdk-lib/aws-dynamodb';
import { Code, Function as LambdaFn, Runtime } from 'aws-cdk-lib/aws-lambda';
import { Rule, Schedule } from 'aws-cdk-lib/aws-events';
import { LambdaFunction } from 'aws-cdk-lib/aws-events-targets';
import { Duration } from 'aws-cdk-lib';

interface Props extends cdk.StackProps {
  table: Table;
  bucketDst: Bucket;
}

export class CleanerStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id, props);

    const fn = new LambdaFn(this, 'CleanerFn', {
      runtime: Runtime.PYTHON_3_11,
      handler: 'cleaner.handler',
      code: Code.fromAsset('lambda'),
      timeout: Duration.seconds(60),
      environment: {
        TABLE_NAME: props.table.tableName,
        BUCKET_DST: props.bucketDst.bucketName,
        GSI_STATUS_AGE: 'GSI_ByStatusAge',
        DISOWNED_LAG_SECONDS: '10',
      },
    });

    props.table.grantReadWriteData(fn);
    props.bucketDst.grantDelete(fn);

    new Rule(this, 'CleanerSchedule', {
      schedule: Schedule.rate(cdk.Duration.minutes(1)),
      targets: [new LambdaFunction(fn)],
    });
  }
}
