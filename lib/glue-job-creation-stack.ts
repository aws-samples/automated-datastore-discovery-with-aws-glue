import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ddb from 'aws-cdk-lib/aws-dynamodb';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as lambda from "aws-cdk-lib/aws-lambda";
import path = require("path");
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as glue from "aws-cdk-lib/aws-glue";
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';


export interface GlueJobCreationStackProps extends cdk.StackProps {
    // This parameters are set from <project-repo>/bin/automated-datastore-discovery-with-aws-glue.ts
    glueTrackerQueue: sqs.Queue;
    trackerTable: ddb.Table;
    s3ScriptBucket: s3.Bucket;
    glueAssetsBucket: s3.Bucket;
    piiOutputTable: ddb.Table;
    ddbScriptBucket: s3.Bucket;
    glueRole: iam.Role;
    rdsScriptBucket: s3.Bucket;
  }

export class GlueJobCreationStack extends cdk.Stack {
  readonly ddbTable: ddb.Table;

  constructor(scope: Construct, id: string, props: GlueJobCreationStackProps) {
    super(scope, id, props);

    // setting some constants
    const glueTrackerQueue = props.glueTrackerQueue;
    const table = props.trackerTable;
    const s3GlueScriptBucket = props.s3ScriptBucket;
    const glueAssetsLocation = props.glueAssetsBucket;
    const ddbOutputTable = props.piiOutputTable;
    const glueETLRole = props.glueRole;
    const ddbGlueScriptBucket = props.ddbScriptBucket;
    const rdsGlueScriptBucket = props.rdsScriptBucket;


    // lambda function to create a dynamoDB entry for a newly onboarded dataset
    const glueInitialFn = new lambda.Function(this, "glueInitialFn", {
        code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/glue-tracking-initial")),
        runtime: lambda.Runtime.PYTHON_3_12,
        timeout: cdk.Duration.minutes(15),
        handler: "app.lambda_handler",
        environment: {
            "SQS_QUEUE_URL": glueTrackerQueue.queueUrl,
            "GLUE_TRACKER_TABLE_NAME": table.tableName
        }
    });
    // create SQS event source
    const sqsEventSource = new SqsEventSource(glueTrackerQueue);
    // trigger Lambda function upon message in SQS queue
    glueInitialFn.addEventSource(sqsEventSource);
    // give permissions to be able to read/write from ddb table
    table.grantReadWriteData(glueInitialFn);


    // create policy statements to attach to lambda function(s) that will create glue jobs
    const lambdaPassRoleStatement = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
          "iam:PassRole",
          ],
      resources: ["*"]
    });
    const lambdaGlueCreatorStatement = new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "glue:CreateJob",
          "glue:GetJob",
          "glue:PutJob",
          "glue:DeleteJob",
          "glue:StartJobRun",
          "glue:CreateWorkflow",
          "glue:CreateTrigger"
            ],
        resources: ["*"]
    });

    // lambda function to create glue job(s) for S3 sources
    const s3GlueCreatorFn = new lambda.Function(this, "s3GlueCreator", {
      code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/s3-glue-job-creator")),
      runtime: lambda.Runtime.PYTHON_3_12,
      timeout: cdk.Duration.minutes(15),
      handler: "app.lambda_handler",
      environment: {
          "GLUE_SCRIPT_BUCKET": s3GlueScriptBucket.bucketName,
          "GLUE_ASSETS_BUCKET": glueAssetsLocation.bucketName,
          "DDB_GLUE_TRACKER_TABLE_NAME": table.tableName,
          "PII_OUTPUT_TABLE_NAME": ddbOutputTable.tableName,
          "GLUE_ROLE_ARN": glueETLRole.roleArn,
          }
    });
    // give permissions to be able to read/write from ddb table
    table.grantFullAccess(s3GlueCreatorFn);   
    // attach necessary policies for glue job creation
    s3GlueCreatorFn.role?.attachInlinePolicy(new iam.Policy(this, 's3CreatorPassRolePolicy', {
      statements: [lambdaPassRoleStatement]
    }));
    s3GlueCreatorFn.role?.attachInlinePolicy(new iam.Policy(this, 's3CreatorGluePolicy', {
      statements: [lambdaGlueCreatorStatement]
    }));
    const s3GlueCreatorRule = new events.Rule(this, 's3GlueCreatorRule', {
      schedule: events.Schedule.cron({
        minute: '0',
        hour: '1',
      }),
    });
    s3GlueCreatorRule.addTarget(new targets.LambdaFunction(s3GlueCreatorFn));

    // lambda function to create glue job(s) for DynamoDB sources
    const ddbGlueCreatorFn = new lambda.Function(this, "ddbGlueCreator", {
      code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/ddb-glue-job-creator")),
      runtime: lambda.Runtime.PYTHON_3_12,
      timeout: cdk.Duration.minutes(15),
      handler: "app.lambda_handler",
      environment: {
          "GLUE_SCRIPT_BUCKET": ddbGlueScriptBucket.bucketName,
          "GLUE_ASSETS_BUCKET": glueAssetsLocation.bucketName,
          "DDB_GLUE_TRACKER_TABLE_NAME": table.tableName,
          "PII_OUTPUT_TABLE_NAME": ddbOutputTable.tableName,
          "GLUE_ROLE_ARN": glueETLRole.roleArn,
          }
    });
    // give permissions to be able to read/write from ddb table
    table.grantFullAccess(ddbGlueCreatorFn);      
    // attach necessary policies for glue job creation
    ddbGlueCreatorFn.role?.attachInlinePolicy(new iam.Policy(this, 'ddbCreatorPassRolePolicy', {
      statements: [lambdaPassRoleStatement]
    }));
    ddbGlueCreatorFn.role?.attachInlinePolicy(new iam.Policy(this, 'ddbCreatorGluePolicy', {
      statements: [lambdaGlueCreatorStatement]
    }));
    const ddbGlueCreatorRule = new events.Rule(this, 'ddbGlueCreatorRule', {
      schedule: events.Schedule.cron({
        minute: '0',
        hour: '1',
      }),
    });
    ddbGlueCreatorRule.addTarget(new targets.LambdaFunction(ddbGlueCreatorFn));


    // lambda function to create glue job(s) for RDS sources
    const rdsGlueCreatorFn = new lambda.Function(this, "rdsGlueCreator", {
      code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/rds-glue-job-creator")),
      runtime: lambda.Runtime.PYTHON_3_12,
      timeout: cdk.Duration.minutes(15),
      handler: "app.lambda_handler",
      environment: {
          "GLUE_SCRIPT_BUCKET": rdsGlueScriptBucket.bucketName,
          "GLUE_ASSETS_BUCKET": glueAssetsLocation.bucketName,
          "DDB_GLUE_TRACKER_TABLE_NAME": table.tableName,
          "PII_OUTPUT_TABLE_NAME": ddbOutputTable.tableName,
          "GLUE_ROLE_ARN": glueETLRole.roleArn,
          }
    });
    // give permissions to be able to read/write from ddb table
    table.grantFullAccess(rdsGlueCreatorFn);
    // attach necessary policies for glue job creation
    rdsGlueCreatorFn.role?.attachInlinePolicy(new iam.Policy(this, 'rdsCreatorPassRolePolicy', {
      statements: [lambdaPassRoleStatement]
    }));
    rdsGlueCreatorFn.role?.attachInlinePolicy(new iam.Policy(this, 'rdsCreatorGluePolicy', {
      statements: [lambdaGlueCreatorStatement]
    }));
    const rdsGlueCreatorRule = new events.Rule(this, 'rdsGlueCreatorRule', {
      schedule: events.Schedule.cron({
        minute: '0',
        hour: '1',
      }),
    });
    rdsGlueCreatorRule.addTarget(new targets.LambdaFunction(rdsGlueCreatorFn));

    // glue data catalog database for S3 sources
    const s3DBName = 's3_source_db';
    const s3GlueDB = new glue.CfnDatabase(this, s3DBName, {
      catalogId: this.account,
      databaseInput: {
        name: s3DBName,
        description: 'Database for S3 data sources'
      }
    });

    // glue data catalog database for DynamoDB sources
    const ddbCatalogDBName = 'ddb_source_db';
    const ddbCatalogDB = new glue.CfnDatabase(this, ddbCatalogDBName, {
      catalogId: this.account,
      databaseInput: {
        name: ddbCatalogDBName,
        description: 'Database for DynamoDB data sources'
      }
    });

    const lambdaGlueCatalogStatement = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        "glue:CreateTable",
        "glue:CreateCrawler"
          ],
      resources: ["*"]
    });

    // lambda function to create data catalog tables and crawlers for S3 sources
    const s3GlueCatalogCreatorFn = new lambda.Function(this, "s3GlueCatalogCreator", {
      code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/catalog-creator-s3")),
      runtime: lambda.Runtime.PYTHON_3_12,
      timeout: cdk.Duration.minutes(15),
      handler: "app.lambda_handler",
      environment: {
          "DDB_GLUE_TRACKER_TABLE_NAME": table.tableName,
          "GLUE_ROLE_ARN": glueETLRole.roleArn,
          "DATA_CATALOG_DB_NAME": s3DBName
          }
    });
    // give permissions to be able to read/write from ddb table
    table.grantFullAccess(s3GlueCatalogCreatorFn);       
    // attach necessary policies for glue job creation
    s3GlueCatalogCreatorFn.role?.attachInlinePolicy(new iam.Policy(this, 's3CatalogCreatorPassRolePolicy', {
      statements: [lambdaPassRoleStatement]
    }));
    s3GlueCatalogCreatorFn.role?.attachInlinePolicy(new iam.Policy(this, 's3CreatorGlueCatalogPolicy', {
      statements: [lambdaGlueCatalogStatement]
    }));
    const s3GlueCatalogCreatorRule = new events.Rule(this, 's3GlueCatalogCreatorRule', {
      schedule: events.Schedule.cron({
        minute: '0',
        hour: '0',
      }),
    });
    s3GlueCatalogCreatorRule.addTarget(new targets.LambdaFunction(s3GlueCatalogCreatorFn));

    // lambda function to create data catalog tables and crawlers for DynamoDB sources
    const ddbGlueCatalogCreatorFn = new lambda.Function(this, "ddbGlueCatalogCreator", {
      code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/catalog-creator-ddb")),
      runtime: lambda.Runtime.PYTHON_3_12,
      timeout: cdk.Duration.minutes(15),
      handler: "app.lambda_handler",
      environment: {
          "DDB_GLUE_TRACKER_TABLE_NAME": table.tableName,
          "GLUE_ROLE_ARN": glueETLRole.roleArn,
          "DATA_CATALOG_DB_NAME": ddbCatalogDBName
          }
    });
    // give permissions to be able to read/write from ddb table
    table.grantFullAccess(ddbGlueCatalogCreatorFn);     
    // attach necessary policies for glue job creation
    ddbGlueCatalogCreatorFn.role?.attachInlinePolicy(new iam.Policy(this, 'ddbCatalogCreatorPassRolePolicy', {
      statements: [lambdaPassRoleStatement]
    }));
    ddbGlueCatalogCreatorFn.role?.attachInlinePolicy(new iam.Policy(this, 'ddbCreatorGlueCatalogPolicy', {
      statements: [lambdaGlueCatalogStatement]
    }));
    const ddbGlueCatalogCreatorRule = new events.Rule(this, 'ddbGlueCatalogCreatorRule', {
      schedule: events.Schedule.cron({
        minute: '0',
        hour: '0',
      }),
    });
    ddbGlueCatalogCreatorRule.addTarget(new targets.LambdaFunction(ddbGlueCatalogCreatorFn));

  }
}
