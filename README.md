# Archival Storage
## Setup Instructions
prerequisites: Java SE Development Kit 8 (or higher), Maven, Git

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
* the Swagger documentation is accessible at: http://localhost:8080/swagger-ui.html
* The Archival Storage needs at least one logical storage available in order to work. To add a local FS storage:
  * call the POST /api/administration/storage endpoint, e.g. using Swagger administration-api, for example with data:
  * {
      "host": "localhost",
      "location": "*path to the folder to which store the data*",
      "name": "*custom name of the storage*",
      "priority": 1,
      "storageType": "FS"
    }
  * in order to produce the right JSON, Windows paths separators has to be escaped ("location":"d:\test" -> "location":"d:\\test")

#### Tests
* run: *mvn test*
* At this moment, ZFS and FS implementations does not differ and tests are the same, therefore installation of ZFS is optional.
* Test class of Local FS/ZFS or FS/ZFS over NFS do not need additional configuration
  * you can optionally change the test folder location at *test.local.folderpath* section of the property file at */src/test/resources/application.properties*
* Test class for Remote FS/ZFS accessed over SFTP requires additional configuration:
  * on the remote machine create *arcstorage* user and adds its public key located at */src/main/resources/arcstorage.pub*
  * make the user owner of the test folder
  * alter the *test.sftp* section of the property file at */src/test/resources/application.properties* with the host, port and path to the test folder
* Test class for Ceph requires additional configuration:
  * install Ceph and RGW http://docs.ceph.com/docs/master/start/ (installation is not trivial, requires infrastructure of nodes, administrator account and it may take few hours)
  * create a bucket and a user with R/W permissions for that bucket
  * alter the *test.ceph* section of the property file at */src/test/resources/application.properties* with the host, port, bucket name, user key and user secret