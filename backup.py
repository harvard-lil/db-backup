import argparse
import boto3
import subprocess
import os
import errno
import stat
import logging
from datetime import datetime


"""This program backs up Amazon RDS instances by selecting the latest
automated snapshot, creating a new database instance from it, and
dumping the results with mysqldump or pg_dump as necessary.

It requires that your AWS credentials be in place, and your allowed actions
should include:

    "rds:DescribeDBSnapshots",
    "rds:CreateDBSnapshots",
    "rds:DeleteDBSnapshots",
    "rds:RestoreDBInstanceFromDBSnapshot",
    "rds:DescribeDBInstances",
    "rds:ModifyDBInstance",
    "rds:DeleteDBInstance"

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
reach the appropriate port.

"""


parser = argparse.ArgumentParser()
parser.add_argument("instance")
parser.add_argument("database")
parser.add_argument("securitygroup")
parser.add_argument("billableto",
                    help="Value of 'Billable To' tag for accounting")
parser.add_argument("--profile", default="default")
parser.add_argument("--snapshot",
                    help="take a snapshot; ONLY USE WITH MULTI-AZ INSTANCES!",
                    action="store_true")
parser.add_argument("--verbose", help="info-level output",
                    action="store_true")
parser.add_argument("--debug", help="debug-level output",
                    action="store_true")
args = parser.parse_args()

if args.verbose:
    logging.basicConfig(level=logging.INFO)
if args.debug:
    logging.basicConfig(level=logging.info)

logging.info("Connecting to RDS...")
session = boto3.Session(profile_name=args.profile)
client = session.client('rds')

backup_time = datetime.now()
backup_timestamp = backup_time.strftime('%Y%m%d%H%M%S')

if args.snapshot:
    snapshot_identifier = "dbb-{0}-{1}".format(args.instance, backup_timestamp)
    logging.info("Creating snapshot {0}...".format(snapshot_identifier))
    response = client.create_db_snapshot(
        DBSnapshotIdentifier=snapshot_identifier,
        DBInstanceIdentifier=args.instance,
        Tags=[
            {
                'Key': 'Billable To',
                'Value': args.billableto
            }
        ])
    latest = snapshot_identifier
    snaptime = backup_time
    waiter = client.get_waiter('db_snapshot_completed')
    waiter.wait(DBSnapshotIdentifier=snapshot_identifier)
    logging.info("Snapshot created.")
else:
    logging.info("Identifying snapshots...")
    response = client.describe_db_snapshots(
        DBInstanceIdentifier=args.instance,
        SnapshotType='automated')

    latest = max([s['DBSnapshotIdentifier'] for s in response['DBSnapshots']
                  if s['DBSnapshotIdentifier'].startswith("rds:{0}".format(args.instance))])
    logging.info("Latest is {0}".format(latest))
    snaptime = datetime.strptime(
        latest, "rds:{0}-%Y-%m-%d-%H-%M".format(args.instance))

db_instance = "{0}-{1}-fromsnap-{2}".format(
    args.instance,
    backup_timestamp,
    snaptime.strftime('%Y%m%d%H%M%S'))

logging.info("Restoring snapshot to instance {0}".format(db_instance))
response2 = client.restore_db_instance_from_db_snapshot(
    DBInstanceIdentifier=db_instance,
    DBSnapshotIdentifier=latest,
    Tags=[
        {
            'Key': 'Billable To',
            'Value': args.billableto
        }
    ],
    CopyTagsToSnapshot=True)

# wait for db to become available
logging.info("Waiting for instance to become available...")
waiter = client.get_waiter('db_instance_available')
waiter.wait(DBInstanceIdentifier=db_instance)

logging.info("Getting instance information...")
response3 = client.describe_db_instances(DBInstanceIdentifier=db_instance)

engine = response3['DBInstances'][0]['Engine']
host = response3['DBInstances'][0]['Endpoint']['Address']
port = response3['DBInstances'][0]['Endpoint']['Port']
user = response3['DBInstances'][0]['MasterUsername']

logging.info("Modifying instance with security group {0}".format(args.securitygroup))
response4 = client.modify_db_instance(
    DBInstanceIdentifier=db_instance,
    VpcSecurityGroupIds=[args.securitygroup])

try:
    os.makedirs(os.path.join(os.getcwd(), args.instance))
    logging.info("Created directory {0}".format(args.instance))
except OSError as e:
    if e.errno != errno.EEXIST:
        raise

flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
mode = stat.S_IRUSR | stat.S_IWUSR

logging.info("Dumping database...")
if engine == 'mysql':
    mycnf = os.path.join(os.getcwd(), '.{0}.my.cnf'.format(args.instance))
    # https://stackoverflow.com/a/15015748/4074877
    with os.fdopen(os.open(os.path.join(os.getcwd(),
                                   args.instance,
                                   '{0}.sql.xz'.format(db_instance)),
                           flags,
                           mode),
                   'w') as f:
        logging.info("Using {0}".format(mycnf))
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
    logging.info("Using {0}".format(pgpass))
    dumpfile = os.path.join(os.getcwd(),
                            args.instance,
                            '{0}.dump'.format(db_instance))
    fd = os.open(dumpfile, flags, mode)
    os.close(fd)
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
                                  dumpfile],
                                 env=d)

logging.info("Deleting instance {0}".format(db_instance))
response5 = client.delete_db_instance(
    DBInstanceIdentifier=db_instance,
    SkipFinalSnapshot=True
)

# is this necessary? the creation of the new instance also
# produces another snapshot we might want to delete
if args.snapshot:
    logging.info("Deleting snapshot...")
    response6 = client.delete_db_snapshot(
        DBSnapshotIdentifier=snapshot_identifier)

logging.info("Done.")
