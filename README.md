# ChubaoFS

[![Build Status](https://travis-ci.org/chubaofs/chubaofs.svg?branch=master)](https://travis-ci.org/chubaofs/chubaofs)
[![LICENSE](https://img.shields.io/github/license/chubaofs/chubaofs.svg)](https://github.com/chubaofs/chubaofs/blob/master/LICENSE)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![Go Report Card](https://goreportcard.com/badge/github.com/chubaofs/chubaofs)](https://goreportcard.com/report/github.com/chubaofs/chubaofs)
[![Docs](https://readthedocs.org/projects/chubaofs/badge/?version=latest)](https://chubaofs.readthedocs.io/en/latest/?badge=latest)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fchubaofs%2Fcfs.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fchubaofs%2Fcfs?ref=badge_shield)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/2761/badge)](https://bestpractices.coreinfrastructure.org/projects/2761)

<img src="https://user-images.githubusercontent.com/47099843/55525970-bf53d880-56c5-11e9-8c28-55d208859824.png" width="400" height="293" />

## Overview

ChubaoFS (储宝文件系统 in Chinese) is a distributed file system and object storage service for cloud native applications. It is hosted by the [Cloud Native Computing Foundation](https://cncf.io) (CNCF) as a [sandbox](https://www.cncf.io/sandbox-projects/) project.

ChubaoFS has been commonly used as the underlying storage infrastructure for online applications, database or data processing services and machine learning jobs orchestrated by Kubernetes. 
An advantage of doing so is to separate storage from compute - one can scale up or down based on the workload and independent of the other, providing total flexibility in matching resources to the actual storage and compute capacity required at any given time.


Some key features of ChubaoFS include:

- Scale-out metadata management

- Strong replication consistency

- Specific performance optimizations for large/small files and sequential/random writes

- Multi-tenancy

- POSIX-compatible and mountable

- S3-compatible object storage interface


We are committed to making ChubaoFS better and more mature. Please stay tuned. 

## Document

https://chubaofs.readthedocs.io/en/latest/

https://chubaofs.readthedocs.io/zh_CN/latest/

## Build


```
$ git clone http://github.com/chubaofs/chubaofs.git
$ cd chubaofs
$ make
```

If the build succeeds, `cfs-server` and `cfs-client` can be found in `build/bin`

## Docker

A helper tool called `run_docker.sh` (under the `docker` directory) has been provided to run ChubaoFS with [docker-compose](https://docs.docker.com/compose/).


```
$ docker/run_docker.sh -r -d /data/disk
```

Note that **/data/disk** can be any directory but please make sure it has at least 10G available space. 


To check the mount status, use the `mount` command in the client docker shell:

```
$ mount | grep chubaofs
```


To view grafana monitor metrics, open http://127.0.0.1:3000 in browser and login with `admin/123456`.
 
To run server and client separately, use the following commands:

```
$ docker/run_docker.sh -b
$ docker/run_docker.sh -s -d /data/disk
$ docker/run_docker.sh -c
$ docker/run_docker.sh -m
```

For more usage:

```
$ docker/run_docker.sh -h
```

## Centos 7.x
### Binary    Download
| Platform   | Architecture | URL                                                          |
| ---------- | --------     | ------                                                       |
| GNU/Linux  | 64-bit Intel | http://storage.jd.com/chubaofsrelease/chubaofs_v1.5.0.tar.gz |

## License

ChubaoFS is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
For detail see [LICENSE](LICENSE) and [NOTICE](NOTICE).

[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fchubaofs%2Fcfs.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fchubaofs%2Fcfs?ref=badge_large)

## Reference

Haifeng Liu, et al., CFS: A Distributed File System for Large Scale Container Platforms. SIGMOD‘19, June 30-July 5, 2019, Amsterdam, Netherlands. 

For more information, please refer to https://dl.acm.org/citation.cfm?doid=3299869.3314046 and https://arxiv.org/abs/1911.03001

## Community

- Twitter: [@ChubaoFS](https://twitter.com/ChubaoFS)
- Mailing list: should use chubaofs-users@groups.io
- Slack: [chubaofs.slack.com](https://chubaofs.slack.com/)
