import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as lambda from "aws-cdk-lib/aws-lambda";
import path = require("path");
import * as rds from 'aws-cdk-lib/aws-rds';
import * as iam from 'aws-cdk-lib/aws-iam';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";


export interface RdsDdlAutomationStackProps extends cdk.StackProps {
  ddlTriggerQueue: sqs.Queue;
  rdsCluster: rds.ServerlessCluster;
  dbName: string;
  ddlSourceS3Bucket: s3.Bucket;
  ddlSourceStackName: string;
}

export class RdsDdlAutomationStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: RdsDdlAutomationStackProps) {
    super(scope, id, props);

    // setting some constants
    const ddlTriggerQueue = props.ddlTriggerQueue;
    const rdsCluster = props.rdsCluster;
    const dbName = props.dbName;
    const sourceS3Bucket = props.ddlSourceS3Bucket;
    const ddlSourceStackName = props.ddlSourceStackName;

    // lambda function to deploy DDL on RDS (when it is first created)
    const ddlInitDeployFn = new lambda.Function(this, "ddlDeployFn", {
      code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/rds-ddl-init")),
      runtime: lambda.Runtime.PYTHON_3_12,
      timeout: cdk.Duration.minutes(15),
      handler: "app.lambda_handler",
      environment:{
          "DB_NAME": dbName,
          "SQS_QUEUE_URL": ddlTriggerQueue.queueUrl,
          "DDL_SOURCE_BUCKET": sourceS3Bucket.bucketName
      },
    });
    // grant RDS Data API access to Lambda function
    rdsCluster.grantDataApiAccess(ddlInitDeployFn);
    // create SQS event source
    const ddlEventSource = new SqsEventSource(ddlTriggerQueue);
    // trigger Lambda function upon message in SQS queue
    ddlInitDeployFn.addEventSource(ddlEventSource);
    // give S3 permissions
    sourceS3Bucket.grantRead(ddlInitDeployFn);
    // to be able to list secrets
    ddlInitDeployFn.role?.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName("SecretsManagerReadWrite")
    );

    // lambda function to deploy DDL on RDS (when there is a change to the DDL SQL File)
    const ddlChangeFn = new lambda.Function(this, "ddlChangeFn", {
      code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/rds-ddl-change")),
      runtime: lambda.Runtime.PYTHON_3_12,
      timeout: cdk.Duration.minutes(15),
      handler: "app.lambda_handler",
      environment:{
          "DB_NAME": dbName,
          "SQS_QUEUE_URL": ddlTriggerQueue.queueUrl,
          "DDL_SOURCE_BUCKET": sourceS3Bucket.bucketName
      },
  });
  // grant RDS Data API access to Lambda function
  rdsCluster.grantDataApiAccess(ddlChangeFn);
  // give S3 permissions
  sourceS3Bucket.grantRead(ddlChangeFn);
  // to be able to list secrets
  ddlChangeFn.role?.addManagedPolicy(
    iam.ManagedPolicy.fromAwsManagedPolicyName("SecretsManagerReadWrite")
  );
  // to be able to describe cluster on RDS
  ddlChangeFn.role?.addManagedPolicy(
    iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonRDSReadOnlyAccess")
  );

  /*
        Trigger an event when there is a ExecuteChangeSet call recorded in CloudTrail
        for the DDLSourceRDSStack
  */
  const cfnChangesetRule = new events.Rule(this, 'cfnChangesetRule', {
        eventPattern: {
            "source": ["aws.cloudformation"],
            "detail": {
              "eventSource": ["cloudformation.amazonaws.com"],
              "eventName": ["ExecuteChangeSet"],
              "requestParameters": {
                "stackName": [ddlSourceStackName]
              }
            }
        },
  });
  // Invoke the ddlChangeFn upon a matching event
  cfnChangesetRule.addTarget(new targets.LambdaFunction(ddlChangeFn));
  }
}