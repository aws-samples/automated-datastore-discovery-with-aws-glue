import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import path = require("path");
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';


export class S3SourceStack extends cdk.Stack {

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const merchantBucket = new s3.Bucket(this, 'merchantBucket', {});
    cdk.Tags.of(merchantBucket).add('gdpr-scan', 'true');
    cdk.Tags.of(merchantBucket).add('Processing Activity', 'Legal');
    cdk.Tags.of(merchantBucket).add('Business Function', 'Legal - UK');
    cdk.Tags.of(merchantBucket).add('Purpose of Processing', 'Legal and Regulatory');
    cdk.Tags.of(merchantBucket).add('Data Subjects', 'Business');
    cdk.Tags.of(merchantBucket).add('APP_NAME', 'Test application 2');
    
    new s3deploy.BucketDeployment(this, 'sourceTestS3Deploy', {
        sources: [s3deploy.Source.asset(path.join(__dirname, "../synthetic-data/s3/merchants"))],
        destinationBucket: merchantBucket
    });

    // bucket creation for business info synthetic data
    const bizInfoBucket = new s3.Bucket(this, 'bizInfoBucket', {});
    cdk.Tags.of(bizInfoBucket).add('gdpr-scan', 'true');
    cdk.Tags.of(bizInfoBucket).add('Processing Activity', 'Manage sales plans. performance and reward - Sales');
    cdk.Tags.of(bizInfoBucket).add('Business Function/Product', 'Sales - UK');
    cdk.Tags.of(bizInfoBucket).add('Purpose of Processing', 'Services management');
    cdk.Tags.of(bizInfoBucket).add('Data Subjects', 'Business Customer');
    cdk.Tags.of(bizInfoBucket).add('APP_NAME', 'Test application 1');


    new s3deploy.BucketDeployment(this, 'sourceBizInfoS3Deploy', {
        sources: [s3deploy.Source.asset(path.join(__dirname, "../synthetic-data/s3/business_info"))],
        destinationBucket: bizInfoBucket
    });

    // bucket creation for individual details synthetic data
    const individualDetailsBucket = new s3.Bucket(this, 'individualDetailsBucket', {});
    cdk.Tags.of(individualDetailsBucket).add('gdpr-scan', 'true');
    cdk.Tags.of(individualDetailsBucket).add('Processing Activity', 'Procurement');
    cdk.Tags.of(individualDetailsBucket).add('Business Function', 'Procurement - UK');
    cdk.Tags.of(individualDetailsBucket).add('Purpose of Processing', 'Services management ');
    cdk.Tags.of(individualDetailsBucket).add('Data Subjects', 'Business Customer');
    cdk.Tags.of(individualDetailsBucket).add('APP_NAME', 'Test application 3');

    new s3deploy.BucketDeployment(this, 'sourceIndividualDetailsS3Deploy', {
        sources: [s3deploy.Source.asset(path.join(__dirname, "../synthetic-data/s3/individual_details"))],
        destinationBucket: individualDetailsBucket
    });

  }
}
