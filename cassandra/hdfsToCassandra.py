from cassandra.cluster import Cluster
from pyhdfs import HdfsClient
cluster = Cluster()
session = cluster.connect()
KEYSPACE = "hadhoop"
HOST = 'pi-node11:9870'
PATH = '/user/pi/result_data2/part-00000'
session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % KEYSPACE)
session.set_keyspace(KEYSPACE)
session.execute("CREATE TABLE IF NOT EXISTS hadhoop(type varchar, times int, PRIMARY KEY(type))")
client=HdfsClient(hosts= HOST)
res = client.open(PATH)
for r in res:
    line=str(r,encoding='utf8')
    strs = line.rstrip().split('\t')
    session.execute(
    """
    INSERT INTO hadhoop (type, times)
    VALUES (%s, %s)
    """,
    (strs[0],int(strs[1])))
