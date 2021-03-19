from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()
KEYSPACE = "hadhoop"
session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % KEYSPACE)
session.set_keyspace(KEYSPACE)
session.execute("CREATE TABLE IF NOT EXISTS hadoop(type varchar, times int, PRIMARY KEY(type))")
rows = session.execute("SELECT * FROM hadoop")
print("read data from cassdranda")
for (firstName,lastName, addresse,age) in rows:
    print(firstName,lastName,addresse,age)


#print("insert data")
#session.execute(
#    """
#    INSERT INTO Personnes (nom, prenom, addresse, age)
#    VALUES (%s, %s, %s, %s)
#    """,
#    ("John O'Reilly","Alex","Brest",42)
#) 


#print("update data")
#session.execute(
#    """
#    UPDATE Personnes SET age = %s WHERE nom = %s and prenom = %s
#    """,(18,'DOE','John')
#)


print("delete row")
session.execute(
    """
    DELETE FROM  Personnes WHERE nom = %s and prenom = %s
    """,('DOE','John')
)
