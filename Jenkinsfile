@Library('shared-libraries') _

// Using testCodeCoverageReport from the jacoco-report-aggregation plugin to produce an aggregated code coverage
// report, even though Sonar doesn't seem to be able to make sense of it yet.
def runtests(String javaVersion){
  sh label:'test', script: '''#!/bin/bash
    export JAVA_HOME=$'''+javaVersion+'''
    export GRADLE_USER_HOME=$WORKSPACE/$GRADLE_DIR
    export PATH=$GRADLE_USER_HOME:$JAVA_HOME/bin:$PATH
    cd marklogic-spark-connector
    echo "Waiting for MarkLogic server to initialize."
    sleep 30s
   ./gradlew -i mlDeploy
   echo "Loading data a second time to try to avoid Optic bug with duplicate rows being returned."
   ./gradlew -i mlLoadData
   ./gradlew testCodeCoverageReport || true
  '''
  junit '**/build/**/*.xml'
}

def runSonarScan(String javaVersion){
    sh label:'test', script: '''#!/bin/bash
      export JAVA_HOME=$'''+javaVersion+'''
      export GRADLE_USER_HOME=$WORKSPACE/$GRADLE_DIR
      export PATH=$GRADLE_USER_HOME:$JAVA_HOME/bin:$PATH
      cd marklogic-spark-connector
     ./gradlew sonar -Dsonar.projectKey='marklogic_marklogic-spark-connector_AY1bXn6J_50_odbCDKMX' -Dsonar.projectName='ML-DevExp-marklogic-spark-connector' || true
    '''
}

pipeline{
  agent none
  triggers{
    parameterizedCron(env.BRANCH_NAME == "develop" ? "00 02 * * * % regressions=true" : "")
  }
  parameters{
    booleanParam(name: 'regressions', defaultValue: false, description: 'indicator if build is for regressions')
  }
  options {
    checkoutToSubdirectory 'marklogic-spark-connector'
    buildDiscarder logRotator(artifactDaysToKeepStr: '7', artifactNumToKeepStr: '', daysToKeepStr: '30', numToKeepStr: '')
  }
  environment{
    JAVA17_HOME_DIR="/home/builder/java/jdk-17.0.2"
    GRADLE_DIR   =".gradle"
    DMC_USER     = credentials('MLBUILD_USER')
    DMC_PASSWORD = credentials('MLBUILD_PASSWORD')
  }
  stages{
    stage('tests'){
      environment{
        scannerHome = tool 'SONAR_Progress'
      }
      agent {label 'devExpLinuxPool'}
      steps{
        sh label:'mlsetup', script: '''#!/bin/bash
            echo "Removing any running MarkLogic server and clean up MarkLogic data directory"
            sudo /usr/local/sbin/mladmin remove
            docker-compose down -v || true
            sudo /usr/local/sbin/mladmin cleandata
            cd marklogic-spark-connector
            mkdir -p docker/marklogic/logs
            docker-compose up -d --build
          '''
        runtests('JAVA17_HOME_DIR')
        // Disabling Sonar scans until we can figure out how to aggregate code coverage data across
        // multiple Gradle subprojects. Until then, the Sonar 80% check always fails because it's only picking up
        // coverage data from one subproject.
//         withSonarQubeEnv('SONAR_Progress') {
//           runSonarScan('JAVA17_HOME_DIR')
//         }
      }
      post{
        always{
          sh label:'mlcleanup', script: '''#!/bin/bash
            cd marklogic-spark-connector
            docker-compose down -v || true
            sudo /usr/local/sbin/mladmin delete $WORKSPACE/marklogic-spark-connector/docker/marklogic/logs/
          '''
        }
      }
    }
    stage('publish'){
      agent {label 'devExpLinuxPool'}
      when {
        branch 'develop'
      }
      steps{
      	sh label:'publish', script: '''#!/bin/bash
          export JAVA_HOME=$JAVA17_HOME_DIR
          export GRADLE_USER_HOME=$WORKSPACE/$GRADLE_DIR
          export PATH=$GRADLE_USER_HOME:$JAVA_HOME/bin:$PATH
          cp ~/.gradle/gradle.properties $GRADLE_USER_HOME;
          cd marklogic-spark-connector
           ./gradlew publish
        '''
      }
    }
    stage('regressions'){
      agent {label 'devExpLinuxPool'}
      when{
        allOf{
          branch 'develop'
          expression {return params.regressions}
        }
      }
      steps{
            sh label:'mlsetup', script: '''#!/bin/bash
                echo "Removing any running MarkLogic server and clean up MarkLogic data directory"
                sudo /usr/local/sbin/mladmin remove
                sudo /usr/local/sbin/mladmin cleandata
                cd marklogic-spark-connector
                mkdir -p docker/marklogic/logs
                docker-compose down -v || true
                MARKLOGIC_TAG=progressofficial/marklogic-db:latest-11 docker-compose up -d --build
            '''
            runtests('JAVA17_HOME_DIR')
      }
      post{
        always{
          sh label:'mlcleanup', script: '''#!/bin/bash
            cd marklogic-spark-connector
            docker-compose down -v || true
            sudo /usr/local/sbin/mladmin delete $WORKSPACE/marklogic-spark-connector/docker/caddy/
            sudo /usr/local/sbin/mladmin delete $WORKSPACE/marklogic-spark-connector/docker/marklogic/logs/
          '''
        }
      }

    }
  }
}
