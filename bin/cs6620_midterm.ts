#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { StorageStack } from '../lib/storage-stack';
import { ReplicatorStack } from '../lib/replicator-stack';
import { CleanerStack } from '../lib/cleaner-stack';

const app = new cdk.App();

const storage = new StorageStack(app, 'BackupStorageStack', {
  env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION },
});

new ReplicatorStack(app, 'BackupReplicatorStack', {
  table: storage.table,
  bucketSrc: storage.bucketSrc,
  bucketDst: storage.bucketDst,
  env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION },
});

new CleanerStack(app, 'BackupCleanerStack', {
  table: storage.table,
  bucketDst: storage.bucketDst,
  env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION },
});
