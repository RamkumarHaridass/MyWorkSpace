node ('SE151872.devfg.xxx.com') {

  wrap([$class: 'BuildUser']) {

    emailVarToSend = env.BUILD_USER_EMAIL

    if (env.REPORTS.length() > 20) {

        currentBuild.displayName=env.BUILD_USER_LAST_NAME+"_"+env.REPORTS.substring(0,15)+"_"+env.QA_ENVIRONMENT+"#"+currentBuild.number

    }

    else {

    currentBuild.displayName=env.BUILD_USER_LAST_NAME+"_"+env.REPORTS+"_"+env.QA_ENVIRONMENT+"#"+currentBuild.number

  }

}

}

pipeline {

    options {

        buildDiscarder(logRotator(daysToKeepStr: '5'))

    }

    agent {

        label ''

    }

    stages {

        stage('Parameters') {

            steps {

                script {

                    properties ([

                        parameters([

                            [$class: 'ChoiceParameter',

                                    choiceType: 'PT_SINGLE_SELECT',

                                    filterLength: 1,

                                    filterable: false,

                                    name: 'QA_ENVIRONMENT',

                                    script: [

                                        $class: 'GroovyScript',

                                        fallbackScript: [

                                            classpath: [],

                                            sandbox: false,

                                            script:

                                                "return['Could not collect information from groovy']"

                                        ],

                                        script: [

                                            classpath: [],

                                            sandbox: false,

                                            script:

                                                "return['A','B']"

                                        ]

                                    ]

                                ],

                         [$class: 'DynamicReferenceParameter',

                                    choiceType: 'ET_ORDERED_LIST',

                                    name: 'ENVIRONMENT INFORMATION',

                                    referencedParameters: 'QA_ENVIRONMENT',

                                    script:

                                        [$class: 'GroovyScript',

                                        script: 'return["Could not get AMi Information"]',

                                        script: [

                                            script: '''

                                                    if (QA_ENVIRONMENT.equals("A")){

                                                        return["Database:  SQL(sql_lrm) + Snowflake(snowflake_liquidity)", "OCP: https://api.ocp-sai-g1.saifg.xxx.com:6443", "CTHUB_HOST: https://cthub-qa.saifg.xxx.com"]

                                                    }

                                                    else if(QA_ENVIRONMENT.equals("B")){

                                                        return["Database:  SQL(sql_lrm_qat) + Snowflake(snowflake_workzone)", "OCP: https://api.ocp-sai-g2.saifg.xxx.com:6443", "CTHUB_HOST: https://cthub-qa-regression.saifg.xxx.com"]

                                                    }

                                                    '''

                                                ]

                                        ]

                                ],

                                    [$class: 'ChoiceParameter',

                                    choiceType: 'PT_CHECKBOX',

                                    description: 'Select required scenarios from the Dropdown List',

                                    name: 'SCENARIOS',

                                    script:

                                        [$class: 'GroovyScript',

                                        fallbackScript: [

                                                classpath: [],

                                                sandbox: false,

                                                script: "return['Could not collect information from groovy']"

                                                ],

                                        script: [

                                                classpath: [],

                                                sandbox: false,

                                                script: '''

                                                return['Generate:selected','Download:selected','Official:selected','Unofficial:selected','Adjust:selected','multiadjust:selected','Refresh:selected','Unadjust:selected']

                                                '''

                                            ]

                                    ]

                                ],

                            [$class: 'ChoiceParameter',

                                    choiceType: 'PT_SINGLE_SELECT',

                                    filterLength: 1,

                                    filterable: true,

                                    name: 'REGION',

                                    script: [

                                        $class: 'GroovyScript',

                                        fallbackScript: [

                                            classpath: [],

                                            sandbox: false,

                                            script:

                                                "return['Could not collect information from groovy']"

                                        ],

                                        script: [

                                            classpath: [],

                                            sandbox: false,

                                            script:

                                                "return['ALL','HO','EMEA','APAC','AMRS','CUSTOM_DI','CUSTOM_USER_SELECT']"

                                        ]

                                    ]

                                ],                  

                            [$class: 'CascadeChoiceParameter',

                                    choiceType: 'PT_CHECKBOX',

                                    description: 'Select Feed Name from the Dropdown List',

                                    name: 'REPORTS',

                                    referencedParameters: 'REGION',

                                    script:

                                        [$class: 'GroovyScript',

                                        fallbackScript: [

                                                classpath: [],

                                                sandbox: false,

                                                script: "return['Could not collect information from groovy']"

                                                ],

                                        script: [

                                                classpath: [],

                                                sandbox: false,

                                                script: '''

                                                if (REGION.equals("HO")){

                                    return["EBET","EDTF","EGL_REGIONAL","ENCCF","ENSFR","H4_From_ELCR_New","ILM","LCR","LCRBYLE","LCR_Monthly","Mobile_Report","SMART_MGMT_GL","Sources_Uses","Sources_Uses_LCRBYLE","SynGL_DS_LandD","SynGL_Res_Variance","SynLRM_Bedford","SynLRM_HO","SynLRM_IntraGroup"]

                                                }

                                                else if(REGION.equals("AMRS")){

                                                    return["FBO","FBO_MONTHLY_REDUCED","RegYY_AMRS","RegYY_CNB_GBANK"]

                                                }

                                                else if(REGION.equals("APAC")){

                                                    return["APRA_LCR","HK_LMR","SG_LCR"]

                                                else{

                                                    return["ALL:selected"]

                                                } 

                                                '''

                                            ]

                                    ]

                                ],

                        string(name: 'UNIX_ENV', defaultValue: 'qa'),

                        string(name: 'BUSINESS_DATE',defaultValue: '2025-03-20', description: 'Business date on which reports should be generated'),

                            ])

                        ])

                }

            }

        }

           

        stage ('Test Creation') {

            steps {

                script {

                  bat (

                        script: 'python scripts/Python_Files/reportTestCaseCreator.py -f scripts/Config/AutomationConfig.xlsx -r %REGION% -z %SCENARIOS% -t %REPORTS% -a testcases/CTHub_T_Reports_[REPORT_NAME].robot -l testcases/',

                        returnStatus: true

                           )

                }

            }

        }

        stage ('Test Execution') {

            steps {

                script {

                  withCredentials([

                  sshUserPrivateKey(credentialsId: 'SP_TGM0_SNOWFLAKE_LIQUIDITY_QA_KEYPAIR', keyFileVariable: 'SSH_KEY_FILE_A', usernameVariable: 'SSH_USERNAME_A'),

                      sshUserPrivateKey(credentialsId: 'SP_TGM0_ADMIN_QA_SNOWFLAKE_WZ_KEYPAIR', keyFileVariable: 'SSH_KEY_FILE_B', usernameVariable: 'SSH_USERNAME_B'),

                  ]) {

                  if (REGION == 'ALL') {

                  FAILED_COUNT = bat (

                        script: 'pabot --processes %NUMBER_OF_PROCESS% --ordering testcases/Depend.txt --removekeywords WUKS --outputdir results/ -v SNOWFLAKE_B_PRIVATE_KEY:%SSH_KEY_FILE_B% -v SNOWFLAKE_A_PRIVATE_KEY:%SSH_KEY_FILE_A% -v SCENARIOS:%SCENARIOS% -v SNOWFLAKE_PASSWORD_WZ:%Snow_input_Pass_WZ%  -v PABOT_EXECUTE:True -v RERUN:False -v JOB_URL:%JOB_URL%/%BUILD_NUMBER%/execution/node/3/ws/ -v BUILD_URL:%BUILD_URL%/console -v BUSINESS_DATE:%BUSINESS_DATE% -v QA_ENVIRONMENT:%QA_ENVIRONMENT% -v BUILD_NUMBER:%BUILD_NUMBER% -v UNIX_ENV:%UNIX_ENV% -v UNIX_USER_NAME:%Unix_input_User% -v UNIX_PWD:%Unix_input_Pass% -v OCP_USER:%OCP_input_User% -v OCP_PASSWORD:%OCP_input_Pass% -v SNOWFLAKE_PASSWORD:"%Snow_input_Pass%" -v VERTICA_PASSWORD:%Vertica_input_Pass% -v SQL_PASSWORD:%Sql_input_Pass% testcases/CTHub_Reports_*.robot',

                        returnStatus: true

                           )

                     }

                  if (FAILED_COUNT != 0) {

                   catchError(buildResult: 'SUCCESS', stageResult: 'FAILURE') {

                  bat "exit 1"

                  }

               }

                }

            }

        }    

        stage ('Update qTest') {

            steps {

                script {

               if (env."EXECUTION TYPE" != 'Skip') {

               bat 'del /f scripts\\qtest-utility\\qTest.json'

               bat 'del /f qTest.json'

               bat 'del /f scripts\\qtest-utility\\output.xml'

               bat 'copy /Y results\\output.xml scripts\\qtest-utility\\'

               bat 'copy /Y results\\log.html scripts\\qtest-utility\\'

                    bat 'python scripts/qtest-utility/qtestJson.py output.xml'

               bat 'copy /Y qTest.json scripts\\qtest-utility\\'

               bat 'python scripts\\qtest-utility\\main.py qTest.json'

               }else {

                        echo "QTest Update is Skipped"

                        catchError(buildResult: 'SUCCESS', stageResult: 'NOT_BUILT') {

                            bat "exit 1"

                        }                

                    }

              

                }

            }

        }

        stage ('Publish Job Status') {

            steps {

                script {

                  if (FAILED_COUNT != 0) {

                   catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {

                  bat "exit 1"

                  }

               }

                }

            }

        }       

    }

    post {

        always {

            robot archiveDirName: 'robot-plugin',

            outputPath: 'results/',

            overwriteXAxisLabel: ''

        }

        success {

            sendEmailNotify ()

        }

        failure {

            sendEmailNotify ()

        }

    }

}

void sendEmailNotify () {

      wrap([$class: 'BuildUser']) {

    emailVarToSend = env.BUILD_USER_EMAIL

    if (env.BUILD_USER_LAST_NAME == 'Trigger') {

        emailVarToSend = 'ramkumar.haridass@xxx.com'

  }

  }

    emailext attachmentsPattern: '**/FinalReport/${BUILD_NUMBER}/*.*',

   body: '''Hi,<br><br>

Report Regression completed.<br>

Environment : $QA_ENVIRONMENT<br>

Business Date : $BUSINESS_DATE <br>

Region : $REGION<br>

Report[s] : $REPORTS<br>

Final report attached along with this email.<br>

<a href="$PROJECT_URL/$BUILD_NUMBER/console">Click Here To Navigate to Job</a><br>

<br><br>

Thanks''',

mimeType: 'text/html',

subject: 'Reports Regression Completed : Build #$BUILD_NUMBER, Status :- ${BUILD_STATUS}',

to: emailVarToSend

}

-------------------------------------------------------

pipeline {

    options {

        buildDiscarder(logRotator(daysToKeepStr: '15')) // Retain builds for 15 days

        disableConcurrentBuilds() // Prevent concurrent builds

    }

 

    parameters {

        string(name: 'BASE_MGMT_TABLE', description: 'Mgmt table name for base') // Single parameter

    }

 

    agent {

        label 'QA-SE151872' // Specify the Jenkins agent label

    }

 

    stages {

        stage('Amount Sum') { // Single stage

            steps {

                script {

                    echo "Running Amount Sum Stage"

                    def buildNumberUpdated = 1 + env.BUILD_NUMBER // Update build number for tracking

 

                    // Define the query for the Amount Sum stage

                    def amountQuery = """SELECT SUM(amount_amountValue) AS amount_amountValue

                                         FROM ${params.BASE_MGMT_TABLE}

                                         WHERE businessdate = '2025-05-14'"""

                   

                    // Define keys for comparison

                    def keys = "amount_amountValue"

 

                    // Execute the stage

                    executeStage(buildNumberUpdated, amountQuery, keys)

                }

            }

        }

    }

}

 

// Helper function to execute a stage

void executeStage(buildNumber, query, keys) {

    withCredentials([

        usernamePassword(credentialsId: 'SP_TGM0_ADMIN_SNOWFLAKE_QA', passwordVariable: 'Snow_input_Pass', usernameVariable: 'Snow_input_User')

    ]) {

        FAILED_COUNT = bat(

            script: """ robot --outputdir results/ --removekeywords WUKS

                        -v BASE_QUERY:"${query}" -v COMPARISON_KEYS:"${keys}" -v BUILD_NUMBER:${buildNumber}

                        testcases/DataCompare.robot""",

            returnStatus: true

        )

        if (FAILED_COUNT != 0) {

            catchError(buildResult: 'SUCCESS', stageResult: 'UNSTABLE') {

                bat "exit 1"

            }

        }

    }

}

------------------------------------

pipeline {

    options {

        buildDiscarder(logRotator(daysToKeepStr: '7')) // Retain builds for 7 days

        disableConcurrentBuilds() // Prevent concurrent builds

    }

 

    agent {

        label 'master' // Specify the Jenkins agent label

    }

 

    parameters {

        booleanParam(name: 'CTHUBMS', defaultValue: false, description: 'Run CTHUBMS stage') // Single parameter

    }

 

    stages {

        stage('CTHUBMS') { // Single stage

            steps {

                script {

                    if (params.CTHUBMS) { // Check if CTHUBMS is enabled

                        echo "Running CTHUBMS Stage"

 

                        // Define the release and job parameters

                        def RELEASE = "RELEASE_1.0" // Example release value

                        def jobRun = build job: 'Reports_Regression', parameters: [

                            string(name: 'QA_ENVIRONMENT', value: "A"),

                        ], propagate: false

 

                        // Handle job result

                        def cthubmsResult = jobRun.result

                        if (cthubmsResult == 'FAILURE') {

                            catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {

                                sh "exit 1"

                            }

                        }

                    } else {

                        echo "CTHUBMS stage skipped because it is marked as False"

                        catchError(buildResult: 'SUCCESS', stageResult: 'NOT_BUILT') {

                            sh "exit 1"

                        }

                    }

                }

            }

        }

    }

 

    post {

        always {

            echo "Cleaning up workspace..."

            cleanWs() // Clean up the workspace after the build

        }

        success {

            echo "Build completed successfully!"

        }

        failure {

            echo "Build failed. Please check the logs."

        }

    }

}