Data Subsystem
======================

How To Start DataNode
---------------------

Start a DataNode process by execute the server binary of ChubaoFS you built with ``-c`` argument and specify configuration file. At least 4 data nodes are required in respect to high availability.

.. code-block:: bash

   nohup cfs-server -c datanode.json &


Configurations
--------------

.. csv-table:: Properties
   :header: "Key", "Type", "Description", "Mandatory"

   "role", "string", "Role of process and must be set to *datanode*", "Yes"
   "port", "string", "Port of TCP network to be listen", "Yes"
   "localIP", "string", "IP of network to be choose", "No,If not specified, the ip address used to communicate with the master is used."
   "prof", "string", "Port of HTTP based prof and api service", "Yes"
   "logDir", "string", "Path for log file storage", "Yes"
   "logLevel", "string", "Level operation for logging. Default is *error*", "No"
   "raftHeartbeat", "string", "Port of raft heartbeat TCP network to be listen", "Yes"
   "raftReplica", "string", "Port of raft replicate TCP network to be listen", "Yes"
   "raftDir", "string", "Path for raft log file storage", "No"
   "consulAddr", "string", "Addresses of monitor system", "No"
   "exporterPort", "string", "Port for monitor system", "No"
   "masterAddr", "string slice", "Addresses of master server", "Yes"
   "disks", "string slice", "
   | Format: *PATH:RETAIN*.
   | PATH: Disk mount point. RETAIN: Retain space. (Ranges: 20G-50G.)", "Yes"


**Example:**

.. code-block:: json

   {
       "role": "datanode",
       "port": "6000",
       "prof": "6001",
       "localIP":"192.168.31.174",
       "logDir": "/export/Logs/datanode",
       "logLevel": "info",
       "raftHeartbeat": "9095",
       "raftReplica": "9096",
       "raftDir": "/export/Data/datanode/raft",
       "consulAddr": "http://consul.prometheus-cfs.local",
       "exporterPort": 9512,    
       "masterAddr": [
            "192.168.31.173:80",
            "192.168.31.141:80",
            "192.168.30.200:80"
       ],
        "disks": [
           "/data0:21474836480",
           "/data1:21474836480"
       ]
   }

