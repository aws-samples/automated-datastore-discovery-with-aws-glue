import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from "aws-cdk-lib/aws-ec2";
import path = require("path");
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as ddb from 'aws-cdk-lib/aws-dynamodb';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as cloudtrail from 'aws-cdk-lib/aws-cloudtrail';


export class BaseInfraStack extends cdk.Stack {
  readonly vpc: ec2.Vpc;
  readonly rdsDdlTriggerQueue: sqs.Queue;
  readonly glueJobTrackingQueue: sqs.Queue;
  readonly trackerTable: ddb.Table;
  readonly piiDetectOutputTable: ddb.Table;
  readonly glueIAMRole: iam.Role;
  readonly glueCustomEntityInitial: sqs.Queue;
  readonly rdsGlueInitialQueue: sqs.Queue;
  readonly tagCaptureTable: ddb.Table;
  readonly rdsGlueSecGroup: ec2.SecurityGroup;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    /* 
    capturing region env var to know which region to deploy this infrastructure

    NOTE - the AWS profile that is used to deploy should have the same default region
    */
    const regionPrefix = process.env.CDK_DEFAULT_REGION || 'us-east-1';
    console.log(`CDK_DEFAULT_REGION: ${regionPrefix}`);

    // create VPC to deploy the infrastructure in
    const vpc = new ec2.Vpc(this, "infraNetwork", {
      ipAddresses: ec2.IpAddresses.cidr('10.80.0.0/20'),
      availabilityZones: [`${regionPrefix}a`, `${regionPrefix}b`, `${regionPrefix}c`],
      subnetConfiguration: [
          {
            name: "public",
            subnetType: ec2.SubnetType.PUBLIC,
          },
          {
            name: "private",
            subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
          }
      ],
    });
    this.vpc = vpc;

    // trail for logging AWS API events
    const trail = new cloudtrail.Trail(this, 'myCloudTrail', {
      managementEvents: cloudtrail.ReadWriteType.ALL
    });

    const rdsDdlDetectionQueue = new sqs.Queue(this, 'rdsDdlDetectionQueue', {
      queueName: "RDS_DDL_Detection_Queue",
      visibilityTimeout: cdk.Duration.minutes(30)
    });
    this.rdsDdlTriggerQueue = rdsDdlDetectionQueue;
    
    // Create DynamoDB table for tracking Glue jobs against data sources
    const ddbTableName = "glueJobTracker";
    const table = new ddb.Table(this, ddbTableName, {
        tableName: ddbTableName,
        partitionKey: { name: "id", type: ddb.AttributeType.STRING },
        billingMode: ddb.BillingMode.PAY_PER_REQUEST,
        encryption: ddb.TableEncryption.AWS_MANAGED,
      });
    this.trackerTable = table;

    const outputDDBTableName = "piiDetectionOutputTable";
    // defining the DynamoDB source for outputting the data for the job run
    const outputDDBTable = new ddb.Table(this, outputDDBTableName, {
        tableName: outputDDBTableName,
        partitionKey: { name: "id", type: ddb.AttributeType.STRING },
        billingMode: ddb.BillingMode.PAY_PER_REQUEST,
        encryption: ddb.TableEncryption.AWS_MANAGED,
    });
    this.piiDetectOutputTable = outputDDBTable;

    const tagCaptureTableName = "tagCaptureTable";
    const ddbTagCaptureTable = new ddb.Table(this, tagCaptureTableName, {
        tableName: tagCaptureTableName,
        partitionKey: { name: "id", type: ddb.AttributeType.STRING },
        sortKey: { name: "data_catalog_table_name", type: ddb.AttributeType.STRING },
        billingMode: ddb.BillingMode.PAY_PER_REQUEST,
        encryption: ddb.TableEncryption.AWS_MANAGED,
    });
    this.tagCaptureTable = ddbTagCaptureTable;

    const glueTrackingQueue = new sqs.Queue(this, 'glueTrackingQueue', {
      queueName: "Glue_Tracking_Queue",
      visibilityTimeout: cdk.Duration.minutes(30)
    });
    this.glueJobTrackingQueue = glueTrackingQueue;

    const glueCustomEntityInitialQueue = new sqs.Queue(this, 'glueCustomEntityInitialQueue', {
      queueName: "Glue_Custom_Entity_Initial_Queue",
      visibilityTimeout: cdk.Duration.minutes(30)
    });
    this.glueCustomEntityInitial = glueCustomEntityInitialQueue;

    const rdsGlueInitializer = new sqs.Queue(this, 'rdsGlueInitializer', {
      queueName: "RDS_Glue_Initializer",
      visibilityTimeout: cdk.Duration.minutes(30)
    });
    this.rdsGlueInitialQueue = rdsGlueInitializer;

    const rdsDdlTriggerFn = new lambda.Function(this, "rdsDdlTriggerFn", {
      code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/rds-ddl-trigger")),
      runtime: lambda.Runtime.PYTHON_3_12,
      timeout: cdk.Duration.minutes(15),
      handler: "app.lambda_handler",
      environment:{
        "RDS_DDL_QUEUE_URL": rdsDdlDetectionQueue.queueUrl,
        "RDS_GLUE_QUEUE_URL": rdsGlueInitializer.queueUrl
    },
    });
    // give permission to the function to be able to send messages to the queues
    rdsDdlDetectionQueue.grantSendMessages(rdsDdlTriggerFn);
    rdsGlueInitializer.grantSendMessages(rdsDdlTriggerFn);

    // Trigger an event when there is a RDS CreateDB API call recorded in CloudTrail
    const eventBridgeCreateDBRule = new events.Rule(this, 'eventBridgeCreateDBRule', {
      eventPattern: {
        source: ["aws.rds"],
        detail: {
          eventSource: ["rds.amazonaws.com"],
          eventName: ["CreateDBCluster"]
        }
      },
    });
    // Invoke the rdsDdlTriggerFn upon a matching event
    eventBridgeCreateDBRule.addTarget(new targets.LambdaFunction(rdsDdlTriggerFn));

    const ddbTriggerFn = new lambda.Function(this, "ddbTriggerFn", {
      code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/dynamodb-trigger")),
      runtime: lambda.Runtime.PYTHON_3_12,
      timeout: cdk.Duration.minutes(15),
      handler: "app.lambda_handler",
      environment:{
        "GLUE_TRACKING_QUEUE_URL": glueTrackingQueue.queueUrl,
        "EXCEPTION_TABLE_NAMES": `${ddbTableName},${tagCaptureTableName}`
    },
    });
    // give permission to the function to be able to send messages to the queues
    glueTrackingQueue.grantSendMessages(ddbTriggerFn);
    
    // Trigger an event when there is a CreateTable (DynamoDB) call recorded in CloudTrail
    const eventBridgeCreateDynamoTableBRule = new events.Rule(this, 'eventBridgeCreateDynamoTableBRule', {
      eventPattern: {
        source: ["aws.dynamodb"],
        detail: {
          eventSource: ["dynamodb.amazonaws.com"],
          eventName: ["CreateTable"]
        }
      },
    });
    eventBridgeCreateDynamoTableBRule.addTarget(new targets.LambdaFunction(ddbTriggerFn));

    const s3TriggerFn = new lambda.Function(this, "s3TriggerFn", {
      code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/s3-trigger")),
      runtime: lambda.Runtime.PYTHON_3_12,
      timeout: cdk.Duration.minutes(15),
      handler: "app.lambda_handler",
      environment:{
        "GLUE_TRACKING_QUEUE_URL": glueTrackingQueue.queueUrl,
        "GLUE_CUSTOM_ENTITY_QUEUE_URL": glueCustomEntityInitialQueue.queueUrl
    },
    });
    // give permission to the function to be able to send messages to the queues
    glueTrackingQueue.grantSendMessages(s3TriggerFn);
    glueCustomEntityInitialQueue.grantSendMessages(s3TriggerFn);
    s3TriggerFn.role?.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonS3FullAccess")
    );  
    // Trigger an event when there is a CreateBucket (S3) call recorded in CloudTrail
    const eventBridgeCreateS3BucketRule = new events.Rule(this, 'eventBridgeCreateS3BucketRule', {
      eventPattern: {
        source: ["aws.s3"],
        detail: {
          eventSource: ["s3.amazonaws.com"],
          eventName: ["CreateBucket"]
        }
      },
    });
    eventBridgeCreateS3BucketRule.addTarget(new targets.LambdaFunction(s3TriggerFn));

    //create security group for glue jdbc connection
    const jdbcSecGroupName = "jdbcGlueSecGroup";
    const glueSecGroup = new ec2.SecurityGroup(this, jdbcSecGroupName, {
        securityGroupName: jdbcSecGroupName,
        vpc: vpc,
        allowAllOutbound: true
    });
    // allow inbound connection
    glueSecGroup.connections.allowFrom(glueSecGroup, ec2.Port.allTraffic());
    this.rdsGlueSecGroup = glueSecGroup;

    const glueETLRole = new iam.Role(
      this, 'glueETLRole', {
        assumedBy: new iam.CompositePrincipal(
          new iam.ServicePrincipal('glue.amazonaws.com'),
        ),
        managedPolicies: [
          iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess'),
          iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonDynamoDBFullAccess'),
          iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonRDSFullAccess'),
          iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
          iam.ManagedPolicy.fromAwsManagedPolicyName('AWSGlueConsoleFullAccess')
        ],
        inlinePolicies: {
          secretFetch: new iam.PolicyDocument({
            statements: [
              new iam.PolicyStatement({
                effect: iam.Effect.ALLOW,
                resources: ["*"],
                actions: [
                  "secretsmanager:GetSecretValue"
                ],
              }),
            ],
          }),
        }
      });
      this.glueIAMRole = glueETLRole;

  }
}
