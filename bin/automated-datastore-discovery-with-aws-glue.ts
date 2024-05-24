#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { BaseInfraStack } from '../lib/base-infra-stack';
import { GlueAssetsStack } from '../lib/glue-assets-stack';
import { GlueJobCreationStack } from '../lib/glue-job-creation-stack';
import { S3SourceStack } from '../lib/s3-source-stack';
import { DynamoDBStack } from '../lib/dynamodb-stack';
import { DDLSourceRDSStack } from '../lib/rds-ddl-source-stack';
import { RDSStack } from '../lib/rds-stack';
import { RdsDdlAutomationStack } from '../lib/ddl-deployment-stack';
import { RDSGlueInitializerStack } from '../lib/rds-glue-data-catalog-init-stack';
import { RDSGlueTrackerStack } from '../lib/rds-glue-data-catalog-tracking-stack';
import { ReportingStack } from '../lib/reporting-stack';

const app = new cdk.App();

const baseInfra = new BaseInfraStack(app, 'BaseInfraStack', {});

const glueAssets = new GlueAssetsStack(app, 'GlueAssetsStack', {});

const glueCreation = new GlueJobCreationStack(app, 'GlueJobCreationStack', {
  glueTrackerQueue: baseInfra.glueJobTrackingQueue,
  trackerTable: baseInfra.trackerTable,
  s3ScriptBucket: glueAssets.s3ScriptBucket,
  glueAssetsBucket: glueAssets.glueAssetsBucket,
  ddbScriptBucket: glueAssets.ddbScriptBucket,
  piiOutputTable: baseInfra.piiDetectOutputTable,
  glueRole: baseInfra.glueIAMRole,
  rdsScriptBucket: glueAssets.rdsScriptBucket
});

const s3Source = new S3SourceStack(app, 'S3SourceStack', {});

const ddbSource = new DynamoDBStack(app, 'DynamoDBStack', {});

const rdsStack = new RDSStack(app, 'RDSStack', {
  vpc: baseInfra.vpc,
  glueSecGroup: baseInfra.rdsGlueSecGroup
});

const ddlSource = new DDLSourceRDSStack(app, 'DDLSourceStack', {
  rdsCluster: rdsStack.rdsCluster
});

const ddlDeploy = new RdsDdlAutomationStack(app, 'DeployDDLStack', {
  ddlTriggerQueue: baseInfra.rdsDdlTriggerQueue,
  rdsCluster: rdsStack.rdsCluster,
  dbName: rdsStack.rdsDBName,
  ddlSourceS3Bucket: ddlSource.sourceS3Bucket,
  ddlSourceStackName: ddlSource.stackName
});

const rdsGlueInit = new RDSGlueInitializerStack(app, 'RDSGlueInitStack', {
  glueRole: baseInfra.glueIAMRole,
  rdsCluster: rdsStack.rdsCluster,
  rdsInitialQueue: baseInfra.rdsGlueInitialQueue,
  rdsCatalogDBName: rdsStack.rdsDataCatalogDBName,
  rdsGlueSecGroup: baseInfra.rdsGlueSecGroup
});

const rdsGlueTracker = new RDSGlueTrackerStack(app, 'RDSGlueTrackerStack', {
  rdsCatalogDBName: rdsStack.rdsDataCatalogDBName,
  ddbTable: baseInfra.trackerTable
});

const reportingStack = new ReportingStack(app, 'ReportingStack', {
  outputTable: baseInfra.piiDetectOutputTable,
  trackerTable: baseInfra.trackerTable,
  tagTable: baseInfra.tagCaptureTable
});
