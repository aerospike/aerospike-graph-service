### Benchmark Constants

Replication Factor=1

Expansion Factor=10

Edge Pack=10

Primary Index=64 bytes per record

Secondary Index=16mb overhead, 14 bytes per record

Label Indexes enabled

Secondary Indexes for one Vertex Property

50% of disk should be used as overhead for data and secondary indexes on disk

### 1 TB Dataset

Vertices=16000320000
Sindexed=2000040000

Edges=14000280000

Data=20TB
Primary Index=1113622272000 (1113GB)
Secondary Index=504010080000+16777216*2 (504GB)

#### MMD
RAM=~1717GB
Disk=~20TB

#### DMD (1.25GB Ram overhead due to PI sprigs)
RAM=~504 + 1.25GB
Disk=~20TB + 1113GB

#### DDD (1.25GB Ram overhead due to PI sprigs)
RAM=~1.25GB

Disk=~20TB + 1113GB + 504GB



|       | MMD                     | DMD                    | DDD                   |
| ----- | ----------------------- | ---------------------- | --------------------- |
| 128GB | Disk 2560GB - RAM 220GB | Disk 2702GB - RAM 66GB | Disk 2767GB - RAM 2GB |
| 64GB  | Disk 1280GB - RAM 110GB | Disk 1351GB - RAM 35GB | Disk 1384GB - RAM 2GB |
| 32GB  | Disk 640GB - RAM 55GB   | Disk 676GB - RAM 19GB  | Disk 692GB - RAM 2GB  |
| 16GB  | Disk 320GB - RAM 28GB   | Disk 338GB - RAM 11GB  | Disk 346GB - RAM 2GB  |
| 8GB   | Disk 160GB - RAM 14GB   | Disk 169GB - RAM 7GB   | Disk 173GB - RAM 2GB  |
| 4GB   | Disk 80GB - RAM 7GB     | Disk 85GB - RAM 5GB    | Disk 87GB - RAM 2GB   |
| 2GB   | Disk 40GB - RAM 4GB     | Disk 43GB - RAM 4GB    | Disk 44GB - RAM 2GB   |
| 1GB   | Disk 20GB - RAM 2GB     | Disk 22GB - RAM 3GB    | Disk 22GB - RAM 2GB   |