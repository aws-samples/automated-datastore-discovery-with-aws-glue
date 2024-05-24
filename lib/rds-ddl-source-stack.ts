import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import path = require("path");
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as rds from 'aws-cdk-lib/aws-rds';


export interface DDLSourceRDSStackProps extends cdk.StackProps {
    rdsCluster: rds.ServerlessCluster;
  }

export class DDLSourceRDSStack extends cdk.Stack {
    readonly sourceS3Bucket: s3.Bucket;

  constructor(scope: Construct, id: string, props: DDLSourceRDSStackProps) {
    super(scope, id, props);

    const ddlSourceBucket = new s3.Bucket(this, 'ddlSourceBucket', {
        bucketName: `ddl-source-${props.rdsCluster.clusterIdentifier}`
    });

    this.sourceS3Bucket = ddlSourceBucket;

    new s3deploy.BucketDeployment(this, 'deployDDLSourceRDS', {
        sources: [s3deploy.Source.asset(path.join(__dirname, "../scripts/rds-ddl-sql"))],
        destinationBucket: ddlSourceBucket
    });

  }
}
