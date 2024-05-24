import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import path = require("path");
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';


export class GlueAssetsStack extends cdk.Stack {
    readonly glueAssetsBucket: s3.Bucket;
    readonly s3ScriptBucket: s3.Bucket;
    readonly ddbScriptBucket: s3.Bucket;
    readonly rdsScriptBucket: s3.Bucket;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // bucket for glue assets
    const glueAssets = new s3.Bucket(this, 'glueAssets', {});
    this.glueAssetsBucket = glueAssets;

    // bucket to hold S3 specific glue script 
    const glueS3ScriptBucket = new s3.Bucket(this, 'glueS3ScriptBucket', {});
    this.s3ScriptBucket = glueS3ScriptBucket;
    new s3deploy.BucketDeployment(this, 'sourceS3GlueScriptDeploy', {
        sources: [s3deploy.Source.asset(path.join(__dirname, "../scripts/glue/s3-source"))],
        destinationBucket: glueS3ScriptBucket
    });

      // bucket to hold DynamoDB specific glue script 
      const glueDynamoScriptBucket = new s3.Bucket(this, 'glueDynamoScriptBucket', {});
      this.ddbScriptBucket = glueDynamoScriptBucket;
      new s3deploy.BucketDeployment(this, 'sourceDynamoGlueScriptDeploy', {
          sources: [s3deploy.Source.asset(path.join(__dirname, "../scripts/glue/ddb-source"))],
          destinationBucket: glueDynamoScriptBucket
      });

    // bucket to hold RDS specific glue script 
    const glueRDSScriptBucket = new s3.Bucket(this, 'glueRDSScriptBucket', {});
    this.rdsScriptBucket = glueRDSScriptBucket;
    new s3deploy.BucketDeployment(this, 'sourceRDSGlueScriptDeploy', {
        sources: [s3deploy.Source.asset(path.join(__dirname, "../scripts/glue/rds-source"))],
        destinationBucket: glueRDSScriptBucket
    });

  }
}
