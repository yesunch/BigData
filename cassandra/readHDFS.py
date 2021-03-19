from pyhdfs import HdfsClient
client=HdfsClient(hosts='pi-node11:9870')
res = client.open('/user/pi/result_data2/part-00000')
for r in res:
    line=str(r,encoding='utf8')
    strs = line.rstrip().split('\t')
    print(strs[0])
    print(strs[1])
