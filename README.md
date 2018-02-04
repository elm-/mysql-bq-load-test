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
def exportAndUpload(dbUri, username, password, table, gsBucket = "gs://datalake/upload", bqDataset = "datalake:core") {
    sh "mysql-bq-load-test -d ${dbUri} -u ${username} -p ${password} -t ${table} -o ./"
    sh "gsutil cp ${table}.json ${gsBucket}/${table}.json"
    sh "bq load --source_format=NEWLINE_DELIMITED_JSON --ignore_unknown_values --replace ${bqDataset}.${table} ${gsBucket}/${table}.json ${table}.bqschema"
}

def googleProjectId = 'cpy-srv-datalake'

pipeline {
    agent {
        docker { 
            image 'elmarweber/mysql-bq-load-test:jenkins'
        }
    }
    stages {
        stage('etl') {
            steps {
                configFileProvider([configFile(fileId: 'datalake-service-account', targetLocation: "${env.JENKINS_HOME}/service-account.json")]) {
                    sh "gcloud auth activate-service-account --key-file=${env.JENKINS_HOME}/service-account.json"
                    sh "gcloud config set core/project ${googleProjectId}"
                    exportAndUpload('jdbc:mysql://my-db.local:3306/employees', 'root', 'secret', 'employees')
                }
            }
        }
    }
}
```