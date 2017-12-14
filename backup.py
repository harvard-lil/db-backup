import argparse
import boto3
import subprocess
import os
import errno
from datetime import datetime


"""This program backs up Amazon RDS instances by selecting the latest
automated snapshot, creating a new database instance from it, and
dumping the results with mysqldump or pg_dump as necessary.

It requires that your AWS credentials be in place.

It requires that your database credentials be in place:

For Postgres instances, .dbinstance.pgpass in this directory
must have permissions of 0600, and have a line like

    *:5432:<dbname>:<dbuser>:<password>

where dbinstance and dbname match those supplied on the command line.

For MySQL instances, the file .dbinstance.my.cnf in this directory must
have permissions of 0600, and read:

    [client]
    password=<password>

where dbinstance matches that supplied on the command line.

It requires a security group that allows your IP address or range to
reach the appropriate port. Should this script create the necessary
security group? It would then require greater privileges.

We need a set of tuples of (source, vpc) to determine security group;
it might be better to require the user to supply the SG.

TODO: add option for making a fresh snapshot and using that?

"""


parser = argparse.ArgumentParser()
parser.add_argument("instance")
parser.add_argument("database")
parser.add_argument("securitygroup")
args = parser.parse_args()

print("Connecting to RDS...")
client = boto3.client('rds')

print("Identifying snapshots...")
response = client.describe_db_snapshots(
    DBInstanceIdentifier=args.instance,
    SnapshotType='automated')

latest = max([s['DBSnapshotIdentifier'] for s in response['DBSnapshots']])
print("Latest is {0}".format(latest))

snaptime = datetime.strptime(latest,
                             "rds:{0}-%Y-%m-%d-%H-%M".format(args.instance))

db_instance = "{0}-{1}-fromsnap-{2}".format(
    args.instance,
    datetime.now().strftime('%Y%m%d%H%M%S'),
    snaptime.strftime('%Y%m%d%H%M%S'))

print("Restoring snapshot to instance {0}".format(db_instance))
response2 = client.restore_db_instance_from_db_snapshot(
    DBInstanceIdentifier=db_instance,
    DBSnapshotIdentifier=latest)

# wait for db to become available
print("Waiting for instance to become available...")
waiter = client.get_waiter('db_instance_available')
waiter.wait(DBInstanceIdentifier=db_instance)

print("Getting instance information...")
response3 = client.describe_db_instances(DBInstanceIdentifier=db_instance)

engine = response3['DBInstances'][0]['Engine']
host = response3['DBInstances'][0]['Endpoint']['Address']
port = response3['DBInstances'][0]['Endpoint']['Port']
user = response3['DBInstances'][0]['MasterUsername']

print("Modifying instance with security group {0}".format(args.securitygroup))
response4 = client.modify_db_instance(
    DBInstanceIdentifier=db_instance,
    VpcSecurityGroupIds=[args.securitygroup])

try:
    os.makedirs(os.path.join(os.getcwd(), args.instance))
    print("Created directory {0}".format(args.instance))
except OSError as e:
    if e.errno != errno.EEXIST:
        raise

print("Dumping database...")
if engine == 'mysql':
    mycnf = os.path.join(os.getcwd(), '.{0}.my.cnf'.format(args.instance))
    with open(os.path.join(os.getcwd(),
                           args.instance,
                           '{0}.sql.xz'.format(db_instance)), 'w') as f:
        print("Using {0}".format(mycnf))
        mysqldump = subprocess.Popen(['mysqldump',
                                      '--defaults-extra-file={0}'.format(
                                          mycnf),
                                      '--single-transaction',
                                      '--databases',
                                      args.database,
                                      '-h',
                                      host,
                                      '-u',
                                      user,
                                      '-P',
                                      str(port)], stdout=subprocess.PIPE)
        compress = subprocess.call(['xz',
                                    '--stdout',
                                    '-'],
                                   stdin=mysqldump.stdout, stdout=f)
        mysqldump.wait()
elif engine == 'postgres':
    # .pgpass in this directory must be set to 0600
    # the host entry for each possibility must be *
    # (psycopg2 does not have pg_dump functionality)
    pgpass = os.path.join(os.getcwd(), '.{0}.pgpass'.format(args.instance))
    print("Using {0}".format(pgpass))
    d = dict(os.environ)
    d['PGPASSFILE'] = pgpass
    returncode = subprocess.call(['pg_dump',
                                  '-Fc',
                                  args.database,
                                  '-h',
                                  host,
                                  '-p',
                                  str(port),
                                  '-U',
                                  user,
                                  '-w',
                                  '-f',
                                  os.path.join(os.getcwd(),
                                               args.instance,
                                               '{0}.dump'.format(
                                                   db_instance))],
                                 env=d)

print("Deleting instance {0}".format(db_instance))
response5 = client.delete_db_instance(
    DBInstanceIdentifier=db_instance,
    SkipFinalSnapshot=True
)

print("Done.")
