# Archival Storage
## Setup Instructions
prerequisites: Java SE Development Kit 8 (or higher), Maven, Git, JAVA_HOME environment variable set to the JDK root folder

#### Database (not needed for tests)
* prerequisite: PostgreSQL installed
* create database: *arcstorage*
* create user: *arcstorage*
* with password: *vuji61oilo*
* or edit the values in */src/main/resources/application.yml* file to match your settings

#### Application
* clone the Git repository
* in its root folder execute: *mvn clean package -Dmaven.test.skip=true*
* run the .jar package from the target folder: *java -jar ./target/archival-storage-1.0-SNAPSHOT.jar*
* the HTTP API is accessible at: http://localhost:8080/api
* The Archival Storage needs at least one logical storage available in order to work. To add the storage, call the POST /api/administration/storage endpoint, e.g. using Swagger administration-api.
  * Local FS/ZFS or FS/ZFS over NFS storage configuration:
  * {
      "host": "localhost",
      "location": "*path to data folder*",
      "name": "local storage",
      "priority": 1,
      "storageType": "FS"
    }
  * Ceph storage configuration:
  * {
      "config": "{\"adapterType\":\"S3\", \"userKey\":\"*RGW S3 user key*\",\"userSecret\":\"*RGW S3 user secret*\"}",
      "host": "*ip address of the RGW instance*",
      "location": "*RGW bucket name*",
      "name": "ceph storage",
      "port": *port of the RGW instance*,
      "priority": 1,
      "storageType": "CEPH"
    }
  * Remote FS/ZFS over SFTP configuration:
  * {
        "host": "*ip address of the remote server*",
        "location": "*path to data folder*",
        "name": "sftp storage",
        "port": *SSH port*,
        "priority": 1,
        "storageType": "*for now, FS and ZFS works the same*"
      }
  * In order to produce the right JSON, Windows paths separators has to be escaped ("location":"d:\test" -> "location":"d:\\\test")
  * See Tests section for additional information.
  
#### Swagger Documentation
* the Swagger documentation is accessible at: http://localhost:8080/swagger-ui.html and the static version is in the *apidoc.html* 
* You can use the Swagger to send the requests but the file download link is broken.
* After sending the request, copy the generated Request URL to the address bar of your browser to start the download.
  
#### Tests
* run: *mvn test*
* At this moment, ZFS and FS implementations do not differ and tests are the same, therefore installation of ZFS is optional.
* Test reports are located at the *.target/surefire-reports* folder.
* Tests are configurable by changing the property file at: */src/test/resources/application.properties*.
* Storage Service Tests run on a private infrastructure. To run them, you need to create your own infrastructure (e.g. Ceph cluster) and change the configuration file entries.
* LocalStorageProcessor tests (test for local FS/ZFS or FS/ZFS over NFS) do not need additional configuration
  * you can optionally change the test folder location at *test.local.folderpath* section of the property file
* RemoteStorageProcessor tests (tests for remote FS/ZFS over SFTP) requires additional configuration
  * on the remote machine create the *arcstorage* user and add its public key located at */src/main/resources/arcstorage.pub*
  * create test folder on the remote machine and make the user owner of the test folder
  * alter the *test.sftp* section of the property file with the host, port, path to the test folder and folder separator of the platform (\ for Windows, / for Linux)
* CephS3 tests (tests for Ceph over S3 API) requires additional configuration
  * install Ceph and RGW http://docs.ceph.com/docs/master/start/
    * installation is not trivial, and may take a few hours
    * installation requires infrastructure of nodes and administrator account to manage them
  * create a bucket and a user with R/W permissions for that bucket (see Ceph RGW manual)
  * alter the *test.ceph* section of the property file with the host, port, bucket name, user key and user secret

#### Configuration of storages used for user testing
```
  {
    "name": "sftp storage",
    "host": "192.168.10.60",
    "port": 22,
    "priority": 0,
    "location": "/arcpool/test",
    "storageType": "ZFS",
    "note": null,
    "config": null,
    "reachable": true,
    "id": "01abac74-82f7-4afc-acfc-251f912c5af1"
  },
  {
    "name": "local storage",
    "host": "localhost",
    "port": 0,
    "priority": 1,
    "location": "d:\\bordel",
    "storageType": "FS",
    "note": null,
    "config": null,
    "reachable": true,
    "id": "4fddaf00-43a9-485f-b81a-d3a4bcd6dd83"
  },
  {
    "name": "ceph storage",
    "host": "192.168.10.60",
    "port": 7480,
    "priority": 1,
    "location": "arclib.bucket1",
    "storageType": "CEPH",
    "note": null,
    "config": "{\"adapterType\":\"S3\", \"userKey\":\"BLZBGL9ZDD23WD0GL8V8\",\"userSecret\":\"pPYbINKQxEBLdxhzbycUI00UmTD4uaHjDel1IPui\"}",
    "reachable": false,
    "id": "8c3f62c0-398c-4605-8090-15eb4712a0e3"
  }
```