# mysql-bq-load-test

[![Build Status](https://travis-ci.org/elm-/mysql-bq-load-test.svg?branch=master)](https://travis-ci.org/elm-/mysql-bq-load-test)

Simple script to load a MySQL table into BQ. Extracts schema and data as JSON.

## Usage

```
mysql-bq-load-test
Usage: mysql-bq-load-test [options]

  -d, --database-url <value>
                           the URL, e.g. 'jdbc:mysql://localhost:3306/test'
  -u, --username <value>   
  -p, --password <value>   
  -t, --table <value>      
  -o, --out <value>   
```

### Example Docker

- localhost as host name does not work as it's the docker host, has to be a resolvable host name
- example below binds current directory to output /data

```
docker run --rm -it -v `pwd`:/data elmarweber/mysql-bq-load-test:latest -- -d "jdbc:mysql://database-host:3306/employees" -u root -p secret -t employees -o /data
```

### Example Jenkinsfile job with upload to BigQuery

```
def exportAndUpload(dbUri, username, password, table, gsBucket, bqDataset) {
    sh "mysql-bq-load-test --database-url ${dbUri} -u ${username} -p ${password} -t ${table} -o ./"
    sh "gsutil cp ${table}.json ${gsBucket}/${table}.json"
    sh "bq load --source_format=NEWLINE_DELIMITED_JSON --ignore_unknown_values --replace ${bqDataset}.${table} ${gsBucket}/${table}.json ${table}.bqschema"
}

def googleProjectId = 'datalake'
def googleCredentialsFileId = 'datalake-service-account'
def googleGsBucket = 'gs://datalake/upload'
def googleBqDataset = 'csm'

def dbHost = 'db-host'
def dbPort = '3306'
def dbName = 'employees'
def username = 'root'
//def username = env.MYSQL_USERNAME
def password = 'secret'
//def password = env.MYSQL_PASSWORD
def table = 'employees'

pipeline {
    agent {
        docker { 
            image 'elmarweber/mysql-bq-load-test:jenkins'
        }
    }
    stages {
        stage('etl') {
            steps {
                configFileProvider([configFile(fileId: googleCredentialsFileId, targetLocation: "${env.JENKINS_HOME}/service-account.json")]) {
                    sh "gcloud auth activate-service-account --key-file=${env.JENKINS_HOME}/service-account.json"
                    sh "gcloud config set core/project ${googleProjectId}"
                    exportAndUpload("jdbc:mysql://${dbHost}:${dbPort}/${dbName}", username, password, table, googleGsBucket, "${googleProjectId}:${googleBqDataset}")
                }
            }
        }
    }
}
```