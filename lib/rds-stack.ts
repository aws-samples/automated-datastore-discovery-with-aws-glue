import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as rds from "aws-cdk-lib/aws-rds";
import path = require("path");
import * as glue from "aws-cdk-lib/aws-glue";


export interface RDSStackProps extends cdk.StackProps {
    vpc: ec2.Vpc;
    glueSecGroup: ec2.SecurityGroup;
  }

export class RDSStack extends cdk.Stack {
  readonly rdsCluster: rds.ServerlessCluster;
  readonly rdsDBName: string;
  readonly rdsDataCatalogDBName: string;

  constructor(scope: Construct, id: string, props: RDSStackProps) {
    super(scope, id, props);
    
    // passed in as property
    const vpc = props.vpc;

    // create RDS bits (security group and serverless instance)
    const dbName = "postgres";
    const rdsSecGroupName = "rds-security-group";
    const rdsEngine = rds.DatabaseClusterEngine.auroraPostgres(
      {
        version: rds.AuroraPostgresEngineVersion.VER_13_9
      }
    );
    const rdsSecurityGroup = new ec2.SecurityGroup(this, rdsSecGroupName, {
      securityGroupName: rdsSecGroupName,
      vpc: vpc,
      allowAllOutbound: true,
    });
    rdsSecurityGroup.connections.allowFrom(props.glueSecGroup, ec2.Port.tcp(5432));

    const rdsSource = new rds.ServerlessCluster(this, 'rdsSource', {
      engine: rdsEngine,
      vpc: vpc,
      defaultDatabaseName: dbName,
      enableDataApi: true,
      securityGroups: [rdsSecurityGroup]
    });
    this.rdsCluster = rdsSource;
    this.rdsDBName = dbName;
    cdk.Tags.of(rdsSource).add('Processing Activity', 'Payroll processing');
    cdk.Tags.of(rdsSource).add('Business Function/Product', 'Finance - UK');
    cdk.Tags.of(rdsSource).add('Purpose of Processing', 'Colleague management');
    cdk.Tags.of(rdsSource).add('Data Subjects', 'Colleagues');
    cdk.Tags.of(rdsSource).add('APP_NAME', 'Test application 5');

    const rdsGlueDBName = 'rds_source_db';
    const rdsGlueDB = new glue.CfnDatabase(this, rdsGlueDBName, 
    {catalogId: this.account,
    databaseInput: {
      name: rdsGlueDBName,
      description: 'Database for RDS data sources'
    }}
    );
    this.rdsDataCatalogDBName = rdsGlueDBName;
  }
}
