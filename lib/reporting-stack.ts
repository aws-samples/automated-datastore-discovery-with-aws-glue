import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ddb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from "aws-cdk-lib/aws-lambda";
import path = require("path");
import * as iam from 'aws-cdk-lib/aws-iam';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';


export interface ReportingStackProps extends cdk.StackProps {
    outputTable: ddb.Table;
    tagTable: ddb.Table;
    trackerTable: ddb.Table;
  }

export class ReportingStack extends cdk.Stack {
  readonly ddbTable: ddb.Table;

  constructor(scope: Construct, id: string, props: ReportingStackProps) {
    super(scope, id, props);

    // setting some constants
    const table = props.outputTable;
    const ddbTagTable = props.tagTable;
    const ddbTrackTable = props.trackerTable;

    const lambdaGlueStatement = new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "glue:UpdateTable",
          "glue:GetTable",
          "glue:GetConnection"
            ],
        resources: ["*"]
    });

    // TODO: Add daily / weekly event bridge trigger to this function
    const PIIReportS3Fn = new lambda.Function(this, "PIIReportS3", {
        code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/pii-report")),
        runtime: lambda.Runtime.PYTHON_3_12,
        timeout: cdk.Duration.minutes(15),
        handler: "app.lambda_handler",
        environment: {
            "OUTPUT_TABLE_NAME": table.tableName,
            "DATA_SOURCE_NAME": "s3"
            }
      });
      // give permissions to be able to read/write from ddb table
      table.grantFullAccess(PIIReportS3Fn);      
        PIIReportS3Fn.role?.attachInlinePolicy(new iam.Policy(this, 'lambdaS3GlueStatement', {
            statements: [lambdaGlueStatement]
        }));

    const s3PIIReportRule = new events.Rule(this, 's3PIIReportRule', {
        schedule: events.Schedule.cron({
            minute: '0',
            hour: '5',
        }),
        });
    s3PIIReportRule.addTarget(new targets.LambdaFunction(PIIReportS3Fn));
    

    // TODO: Add daily / weekly event bridge trigger to this function
    const PIIReportDDBFn = new lambda.Function(this, "PIIReportDDB", {
        code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/pii-report")),
        runtime: lambda.Runtime.PYTHON_3_12,
        timeout: cdk.Duration.minutes(15),
        handler: "app.lambda_handler",
        environment: {
            "OUTPUT_TABLE_NAME": table.tableName,
            "DATA_SOURCE_NAME": "dynamodb"
            }
      });
      // give permissions to be able to read/write from ddb table
      table.grantFullAccess(PIIReportDDBFn);      
      PIIReportDDBFn.role?.attachInlinePolicy(new iam.Policy(this, 'lambdaDDBGlueStatement', {
            statements: [lambdaGlueStatement]
        }));

    const ddbPIIReportRule = new events.Rule(this, 'ddbPIIReportRule', {
        schedule: events.Schedule.cron({
            minute: '0',
            hour: '5',
        }),
        });
    ddbPIIReportRule.addTarget(new targets.LambdaFunction(PIIReportDDBFn));

    // TODO: Add daily / weekly event bridge trigger to this function
    const PIIReportRDSFn = new lambda.Function(this, "PIIReportRDS", {
        code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/pii-report")),
        runtime: lambda.Runtime.PYTHON_3_12,
        timeout: cdk.Duration.minutes(15),
        handler: "app.lambda_handler",
        environment: {
            "OUTPUT_TABLE_NAME": table.tableName,
            "DATA_SOURCE_NAME": "rds"
            }
      });
      // give permissions to be able to read/write from ddb table
      table.grantFullAccess(PIIReportRDSFn);      
      PIIReportRDSFn.role?.attachInlinePolicy(new iam.Policy(this, 'lambdaRDSGlueStatement', {
            statements: [lambdaGlueStatement]
        }));

    const rdsPIIReportRule = new events.Rule(this, 'rdsPIIReportRule', {
        schedule: events.Schedule.cron({
            minute: '0',
            hour: '5',
        }),
        });
    rdsPIIReportRule.addTarget(new targets.LambdaFunction(PIIReportRDSFn));

    const tagReportS3Fn = new lambda.Function(this, "tagReportS3Fn", {
        code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/s3-tag-report")),
        runtime: lambda.Runtime.PYTHON_3_9,
        timeout: cdk.Duration.minutes(15),
        handler: "app.lambda_handler",
        environment: {
            "DDB_GLUE_TRACKER_TABLE_NAME": ddbTrackTable.tableName,
            "TAG_REPORT_TABLE_NAME": ddbTagTable.tableName
            }
      });
      // give permissions to be able to read/write from ddb table
      ddbTagTable.grantFullAccess(tagReportS3Fn);
      ddbTrackTable.grantFullAccess(tagReportS3Fn);
      tagReportS3Fn.role?.addManagedPolicy(
        iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonS3FullAccess")
      );

    const s3TagReportRule = new events.Rule(this, 's3TagReportRule', {
        schedule: events.Schedule.cron({
            minute: '0',
            hour: '6',
        }),
        });
    s3TagReportRule.addTarget(new targets.LambdaFunction(tagReportS3Fn));

    const tagReportDDBFn = new lambda.Function(this, "tagReportDDBFn", {
        code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/ddb-tag-report")),
        runtime: lambda.Runtime.PYTHON_3_12,
        timeout: cdk.Duration.minutes(15),
        handler: "app.lambda_handler",
        environment: {
            "DDB_GLUE_TRACKER_TABLE_NAME": ddbTrackTable.tableName,
            "TAG_REPORT_TABLE_NAME": ddbTagTable.tableName
            }
      });
      // give permissions to be able to read/write from ddb table
      tagReportDDBFn.role?.addManagedPolicy(
        iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonDynamoDBFullAccess")
      );

    const ddbTagReportRule = new events.Rule(this, 'ddbTagReportRule', {
        schedule: events.Schedule.cron({
            minute: '0',
            hour: '6',
        }),
        });
    ddbTagReportRule.addTarget(new targets.LambdaFunction(tagReportDDBFn));

    const tagReportRDSFn = new lambda.Function(this, "tagReportRDSFn", {
        code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/rds-tag-report")),
        runtime: lambda.Runtime.PYTHON_3_12,
        timeout: cdk.Duration.minutes(15),
        handler: "app.lambda_handler",
        environment: {
            "DDB_GLUE_TRACKER_TABLE_NAME": ddbTrackTable.tableName,
            "TAG_REPORT_TABLE_NAME": ddbTagTable.tableName
            }
      });
      // give permissions to be able to read/write from ddb table
      tagReportRDSFn.role?.addManagedPolicy(
        iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonDynamoDBFullAccess")
      );
      tagReportRDSFn.role?.addManagedPolicy(
        iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonRDSFullAccess")
      );
      tagReportRDSFn.role?.attachInlinePolicy(new iam.Policy(this, 'rdsReportGlue', {
        statements: [lambdaGlueStatement]
    }));

    const rdsTagReportRule = new events.Rule(this, 'rdsTagReportRule', {
        schedule: events.Schedule.cron({
            minute: '0',
            hour: '6',
        }),
        });
        rdsTagReportRule.addTarget(new targets.LambdaFunction(tagReportRDSFn));

  }
}
