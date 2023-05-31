@Library('shared-libraries') _

def runtests(String mlVersionType, String mlVersion, String javaVersion){
  copyRPM mlVersionType,mlVersion
  setUpML '$WORKSPACE/xdmp/src/Mark*.rpm'
  sh label:'test', script: '''#!/bin/bash
    export JAVA_HOME=$'''+javaVersion+'''
    export GRADLE_USER_HOME=$WORKSPACE/$GRADLE_DIR
    export PATH=$GRADLE_USER_HOME:$JAVA_HOME/bin:$PATH
    cd marklogic-spark-connector
    echo "mlPassword=admin" > gradle-local.properties
   ./gradlew -i mlDeploy
   ./gradlew test || true
  '''
  junit '**/build/**/*.xml'
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
    JAVA8_HOME_DIR="/home/builder/java/openjdk-1.8.0-262"
    JAVA11_HOME_DIR="/home/builder/java/jdk-11.0.2"
    JAVA17_HOME_DIR="/home/builder/java/jdk-17.0.2"
    GRADLE_DIR   =".gradle"
    DMC_USER     = credentials('MLBUILD_USER')
    DMC_PASSWORD = credentials('MLBUILD_PASSWORD')
  }
  stages{
    stage('tests'){
      agent {label 'devExpLinuxPool'}
      steps{
        runtests('Latest','11','JAVA8_HOME_DIR')
      }
    }
    stage('publish'){
      agent {label 'devExpLinuxPool'}
      when {
        branch 'develop'
      }
      steps{
      	sh label:'publish', script: '''#!/bin/bash
          export JAVA_HOME=$JAVA_HOME_DIR
          export GRADLE_USER_HOME=$WORKSPACE/$GRADLE_DIR
          export PATH=$GRADLE_USER_HOME:$JAVA_HOME/bin:$PATH
          cp ~/.gradle/gradle.properties $GRADLE_USER_HOME;
          cd marklogic-spark-connector
           ./gradlew publish
        '''
      }
    }
    stage('regressions'){
      when{
        allOf{
          branch 'develop'
          expression {return params.regressions}
        }
      }
      parallel{
        stage('11-nightly-java11'){
          agent {label 'devExpLinuxPool'}
          steps{
            runtests('Latest','11','JAVA11_HOME_DIR')
          }
        }
        stage('11-nightly-java17'){
          agent {label 'devExpLinuxPool'}
          steps{
            runtests('Latest','11','JAVA17_HOME_DIR')
          }
        }
        stage('10.0-9.5-java11'){
          agent {label 'devExpLinuxPool'}
          steps{
            runtests('Release','10.0-9.5','JAVA11_HOME_DIR')
          }
        }
        stage('10.0-9.5-nightly-java17'){
          agent {label 'devExpLinuxPool'}
          steps{
            runtests('Release','10.0-9.5','JAVA17_HOME_DIR')
          }
        }
      }
    }
  }
}
