import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ddb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from "aws-cdk-lib/aws-lambda";
import path = require("path");
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";


export interface RDSGlueTrackerStackProps extends cdk.StackProps {
    rdsCatalogDBName: string;
    ddbTable: ddb.Table;
  }

export class RDSGlueTrackerStack extends cdk.Stack {
  readonly ddbTable: ddb.Table;

  constructor(scope: Construct, id: string, props: RDSGlueTrackerStackProps) {
    super(scope, id, props);

    // setting some constants
    const catalogDBName = props.rdsCatalogDBName;
    const trackerTable = props.ddbTable;

    const rdsGlueTrackingEntryFn = new lambda.Function(this, "rdsGlueTrackingEntryFn", {
        code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/rds-glue-tracking-initial")),
        runtime: lambda.Runtime.PYTHON_3_12,
        timeout: cdk.Duration.minutes(15),
        handler: "app.lambda_handler",
        environment:{
          "DDB_TABLE_NAME": trackerTable.tableName,
          "CATALOG_DB_NAME": catalogDBName
      },
      });
      // give permission to the function to be able to send messages to the queues
      trackerTable.grantReadWriteData(rdsGlueTrackingEntryFn);
      
      // Trigger an event when there is a CreateTable event in the glue data catalog
      const glueCreateTableRule = new events.Rule(this, 'glueCreateTableRule', {
        eventPattern: {
          source: ["aws.glue"],
          detail: {
            eventSource: ["glue.amazonaws.com"],
            eventName: ["CreateTable"]
          }
        },
      });
      glueCreateTableRule.addTarget(new targets.LambdaFunction(rdsGlueTrackingEntryFn));    


  }
}
