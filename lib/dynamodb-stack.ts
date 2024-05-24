import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ddb from 'aws-cdk-lib/aws-dynamodb';


export class DynamoDBStack extends cdk.Stack {

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const ddbIndividualDetails = "IndividualDetails";
    const individualDetailsTable = new ddb.Table(this, ddbIndividualDetails, {
        tableName: ddbIndividualDetails,
        partitionKey: { name: "id", type: ddb.AttributeType.STRING },
        billingMode: ddb.BillingMode.PAY_PER_REQUEST,
        // might need to see if this works with Glue or not (alternative is to create KMS key separately)
        encryption: ddb.TableEncryption.AWS_MANAGED,
      });
    cdk.Tags.of(individualDetailsTable).add('Processing Activity', 'HR');
    cdk.Tags.of(individualDetailsTable).add('Business Function', 'Human Resources - UK');
    cdk.Tags.of(individualDetailsTable).add('Purpose of Processing', 'Colleague management');
    cdk.Tags.of(individualDetailsTable).add('Data Subjects', 'Colleagues existing');
    cdk.Tags.of(individualDetailsTable).add('APP_NAME', 'Test application 4');

    const ddbNetworkInfo = "NetworkInfo";
    const networkInfoTable = new ddb.Table(this, ddbNetworkInfo, {
        tableName: ddbNetworkInfo,
        partitionKey: { name: "id", type: ddb.AttributeType.STRING },
        billingMode: ddb.BillingMode.PAY_PER_REQUEST,
        // might need to see if this works with Glue or not (alternative is to create KMS key separately)
        encryption: ddb.TableEncryption.AWS_MANAGED,
      });
    cdk.Tags.of(networkInfoTable).add('Processing Activity', 'Manage supplier relationship and performance');
    cdk.Tags.of(networkInfoTable).add('Business Function', 'Procurement - UK');
    cdk.Tags.of(networkInfoTable).add('Purpose of Processing', 'Analytics and Insights');
    cdk.Tags.of(networkInfoTable).add('APP_NAME', 'Test application 8');

  }
}
