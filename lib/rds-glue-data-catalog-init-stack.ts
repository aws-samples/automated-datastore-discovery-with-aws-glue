import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ddb from 'aws-cdk-lib/aws-dynamodb';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as lambda from "aws-cdk-lib/aws-lambda";
import path = require("path");
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as rds from "aws-cdk-lib/aws-rds";


export interface RDSGlueInitializerStackProps extends cdk.StackProps {
    glueRole: iam.Role;
    rdsInitialQueue: sqs.Queue;
    rdsCatalogDBName: string;
    rdsCluster: rds.ServerlessCluster;
    rdsGlueSecGroup: ec2.SecurityGroup;
  }

export class RDSGlueInitializerStack extends cdk.Stack {
  readonly ddbTable: ddb.Table;

  constructor(scope: Construct, id: string, props: RDSGlueInitializerStackProps) {
    super(scope, id, props);

    // setting some constants
    const glueETLRole = props.glueRole;
    const initQueue = props.rdsInitialQueue;
    const catalogDBName = props.rdsCatalogDBName;

    // create policy statements to attach to lambda function(s) 
    const lambdaPassRoleStatement = new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
            "iam:PassRole",
            ],
        resources: ["*"]
    });
    const glueConnectionCrawlerStatement = new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
        "glue:CreateCrawler",
        "glue:CreateConnection"
            ],
        resources: ["*"]
    });

    const rdsDescribeSubnetGroupStatement = new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
            "rds:DescribeDBSubnetGroups"
        ],
        resources: ["*"]
    });
    // lambda function to create JDBC Connection and Crawler for RDS
    const rdsGlueInitializerFn = new lambda.Function(this, "rdsGlueInitializerFn", {
        code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/rds-glue-initial")),
        runtime: lambda.Runtime.PYTHON_3_12,
        timeout: cdk.Duration.minutes(15),
        handler: "app.lambda_handler",
        environment: {
            "SQS_QUEUE_URL": initQueue.queueUrl,
            "CRAWLER_ROLE_ARN": glueETLRole.roleArn,
            "CATALOG_DB_NAME": catalogDBName,
            "CONN_SEC_GROUP_ID": props.rdsGlueSecGroup.securityGroupId,
        }
    });
    // create SQS event source
    const sqsEventSource = new SqsEventSource(initQueue);
    // trigger Lambda function upon message in SQS queue
    rdsGlueInitializerFn.addEventSource(sqsEventSource);
    // grant RDS Data API access to Lambda function
    props.rdsCluster.grantDataApiAccess(rdsGlueInitializerFn);

    // to be able to list secrets
    rdsGlueInitializerFn.role?.addManagedPolicy(
        iam.ManagedPolicy.fromAwsManagedPolicyName("SecretsManagerReadWrite")
    );
    // to be able to describe cluster on RDS
    rdsGlueInitializerFn.role?.addManagedPolicy(
        iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonRDSReadOnlyAccess")
    );
    
    rdsGlueInitializerFn.role?.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName("SecretsManagerReadWrite")
    );       
    // attach necessary policies for glue job creation
    rdsGlueInitializerFn.role?.attachInlinePolicy(new iam.Policy(this, 'passRolePolicy', {
      statements: [lambdaPassRoleStatement]
    }));
    rdsGlueInitializerFn.role?.attachInlinePolicy(new iam.Policy(this, 'glueConnectionCrawlerPolicy', {
      statements: [glueConnectionCrawlerStatement]
    }));
    rdsGlueInitializerFn.role?.attachInlinePolicy(new iam.Policy(this, 'rdsDescribeSubnetGroupPolicy', {
        statements: [rdsDescribeSubnetGroupStatement]
      }));
  }
}
