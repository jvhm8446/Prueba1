import os
from aws_cdk import (
    Stack,
    Duration,
    aws_iam,
    aws_stepfunctions_tasks,
    aws_logs,
    RemovalPolicy,
    aws_apigateway,
    aws_events,
    aws_ssm,
    aws_s3,
)

from constructs import Construct
from aws_cdk.aws_stepfunctions import (
    Fail,
    Succeed,
    Choice,
    Condition,
    StateMachine,
    StateMachineType,
    LogOptions,
    LogLevel,
    Pass,
    TaskInput,
    Map,
    IntegrationPattern,
    JsonPath,
    Parallel,
    CustomState
)
from bci_validation_wfl.helpers.nomenclature import pascal_case
from bci_validation_wfl import lambdas


def build_bci_validation_wfl_step_function(
    scope: Construct,
    state_machine_name: str,
    role: aws_iam.Role,
    bucket_storage: aws_s3.IBucket,
    event_bus_arn: str,
    ecommerce_api_id: str,
    ecommerce_api_key: str,
    bci_proxy_api_id: str,
    bci_proxy_api_key: str,
    bci_connect_sync_api_id: str,
    lendbot_api_id: str,
    lendbot_api_key: str,
    state_machine_arn: str,
    bci_rest_wfl: str,
    support_sns: str
) -> StateMachine:
    # LOGS GROUP FOR STEP FUNCTION
    log_group_sf_name = (
        f'bci-validation-wfl-step_function-log-group-{os.environ.get("STAGE")}'
    )
    log_group_sf = aws_logs.LogGroup(
        scope,
        pascal_case(log_group_sf_name),
        retention=aws_logs.RetentionDays.ONE_MONTH,
        removal_policy=RemovalPolicy.DESTROY,
    )
    event_bus = aws_events.EventBus.from_event_bus_arn(
        scope, f"EventBusARN-{os.environ.get('STAGE')}", event_bus_arn=event_bus_arn
    )
    state_machine_legal_entities = StateMachine.from_state_machine_arn(
        scope,
        f"StepFunctionLegalEntities-{os.environ.get('STAGE')}",
        state_machine_arn=state_machine_arn,
    )
    state_machine_bci_res = StateMachine.from_state_machine_arn(
        scope,
        f"StepFunctionBCIRES-{os.environ.get('STAGE')}",
        state_machine_arn=bci_rest_wfl,
    )
    api_ecommerce = aws_apigateway.RestApi.from_rest_api_id(
        scope,
        f"BciEcommerceApi-{os.environ.get('STAGE')}",
        rest_api_id=ecommerce_api_id,
    )
    api_bci = aws_apigateway.RestApi.from_rest_api_id(
        scope, f"BciApiProxy-{os.environ.get('STAGE')}", rest_api_id=bci_proxy_api_id
    )
    api_notify = aws_apigateway.RestApi.from_rest_api_id(
        scope,
        f"BciConnectSyncApi-{os.environ.get('STAGE')}",
        rest_api_id=bci_connect_sync_api_id,
    )

    lambda_get_legalbot = lambdas.lambda_get_legalbot(scope)
    lambda_check_duration = lambdas.lambda_check_duration(scope)

    aws_ssm.StringParameter(
        scope,
        pascal_case(
            f"bci-validation-wfl-lambda-legalbot-arn-{os.environ.get('STAGE')}-ssm-parameter"),
        parameter_name=f'/{os.environ.get("REGION")}/{os.environ.get("STAGE")}/bci-validation-wfl/lambda-legalbot-arn',
        string_value=lambda_get_legalbot.function_arn,
        tier=aws_ssm.ParameterTier.STANDARD
    )

    task_put_event = aws_stepfunctions_tasks.EventBridgePutEvents(
        scope,
        "Iniciando precalificacion",
        entries=[
            aws_stepfunctions_tasks.EventBridgePutEventsEntry(
                detail=TaskInput.from_object(
                    {
                        "customer": JsonPath.string_at("$.customerCode"),
                        "event": {
                            "eventName": "INICIANDO_PRECALIFICACION",
                            "code": 0,
                            "message": "iniciado proceso de precalificación",
                            "product": JsonPath.string_at("$.product"),
                            "clientId": JsonPath.string_at("$.clientId"),
                            "status": "Precalificacion",
                        },
                    }
                ),
                event_bus=event_bus,
                detail_type="MessageFromStepFunctions",
                source="step.functions",
            )
        ],
        result_path="$.resultSendEvent",
    )

    invoke_ecommerce_api = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Actualizar estado de solicitud a Precalificacion",
        api=api_ecommerce,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.PATCH,
        api_path="/partial-checkout",
        headers=TaskInput.from_object(
            {
                "Content-Type": ["application/json"],
                "x-api-key": [f"{ecommerce_api_key}"],
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "step": 0,
                "productId": 2,
                "data": {
                    "status": "Precalificacion",
                    "bciValidationProcessId": JsonPath.string_at("$.processId"),
                },
            }
        ),
        result_path="$.resultEcommerceApi",
    )
    invoke_step_function = aws_stepfunctions_tasks.StepFunctionsStartExecution(
        scope,
        "Validar si tiene termino de giro-Inicio de actividades en legalEntities",
        state_machine=state_machine_legal_entities,
        integration_pattern=IntegrationPattern.RUN_JOB,
        input=TaskInput.from_object({"rut": JsonPath.string_at("$.rut")}),
        name="ExecutionStepFunctionLegalEntities",
        result_path="$.resultLegalEntities",
        result_selector={"body": JsonPath.string_at("$.Output.result.body")},
        heartbeat=Duration.minutes(1),
        timeout=Duration.seconds(300),
    )
    invoke_bci_api = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Validar con filtros de bci",
        api=api_bci,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.POST,
        auth_type=aws_stepfunctions_tasks.AuthType.IAM_ROLE,
        api_path="/v1/validations",
        headers=TaskInput.from_object(
            {
                "Content-Type": ["application/json"],
                "x-api-key": [f"{bci_proxy_api_key}"],
            }
        ),
        request_body=TaskInput.from_object(
            {
                "Rut": JsonPath.string_at(
                    "$.resultLegalEntities.body.EntidadLegal.Rut"
                ),
                "RazonSocial": JsonPath.string_at(
                    "$.resultLegalEntities.body.EntidadLegal.RazonSocial"
                ),
                "CanalOrigen": "Datamart",
                "Personas": [],
            }
        ),
        result_selector={
            "body": JsonPath.string_at("$.ResponseBody"),
        },
        result_path="$.resultBciApi",
    )

    save_legal_entities = aws_stepfunctions_tasks.CallAwsService(
        scope,
        "Guardar LegalEntities en S3",
        service="s3",
        action="putObject",
        iam_action="s3:PutObject",
        iam_resources=[f"{bucket_storage.bucket_arn}/*"],
        parameters={
            "Body": JsonPath.string_at("$.resultLegalEntities.body.EntidadLegal"),
            "Bucket": bucket_storage.bucket_name,
            "Key": JsonPath.format(
                "onboarding/{}/{}/LegalEntities/EntidadLegal.json",
                JsonPath.string_at(
                    "$.resultLegalEntities.body.EntidadLegal.Rut"),
                JsonPath.string_at("$.processId"),
            ),
            "ContentType": "application/json",
        },
        result_path=JsonPath.DISCARD,
    )

    save_bci_filters = aws_stepfunctions_tasks.CallAwsService(
        scope,
        "Guardar Consulta Api de filtros BCI en S3",
        service="s3",
        action="putObject",
        iam_action="s3:PutObject",
        iam_resources=[f"{bucket_storage.bucket_arn}/*"],
        parameters={
            "Body": JsonPath.string_at("$.resultBciApi.body"),
            "Bucket": bucket_storage.bucket_name,
            "Key": JsonPath.format(
                "onboarding/{}/{}/BciApiFilters/ValidCompanyApiFilters.json",
                JsonPath.string_at(
                    "$.resultLegalEntities.body.EntidadLegal.Rut"),
                JsonPath.string_at("$.processId"),
            ),
            "ContentType": "application/json",
        },
        result_path=JsonPath.DISCARD,
    )
    # TODO: Catch generales para salvas del ecommerce
    catch_save_ecommerce_branch_0 = Succeed(
        scope, "No es posible actualizar Solicitud", comment="Solicitud en estado de rechazo")
    catch_save_ecommerce_branch_1 = Succeed(
        scope, "No es posible actualizar estado Solicitud", comment="Solicitud en estado de rechazo")

    save_ecommerce = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Guardar datos de validacion LegalEntities",
        api=api_ecommerce,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.PATCH,
        api_path="/partial-checkout",
        headers=TaskInput.from_object(
            {
                "Content-Type": ["application/json"],
                "x-api-key": [f"{ecommerce_api_key}"],
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "step": 0,
                "productId": 2,
                "data": {
                    "companyValidation": {
                        "legalEntitiesUrl": JsonPath.format(
                            "https://{}/onboarding/{}/{}/LegalEntities/EntidadLegal.json",
                            bucket_storage.bucket_name,
                            JsonPath.string_at(
                                "$.resultLegalEntities.body.EntidadLegal.Rut"
                            ),
                            JsonPath.string_at("$.processId"),
                        ),
                        "companyName": JsonPath.string_at(
                            "$.resultLegalEntities.body.EntidadLegal.RazonSocial"
                        ),
                    }
                },
            }
        ),
        result_path=JsonPath.DISCARD,
    )
    save_ecommerce_bci_filters = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Guardar datos de validacion Api de filtros de bci",
        api=api_ecommerce,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.PATCH,
        api_path="/partial-checkout",
        headers=TaskInput.from_object(
            {
                "Content-Type": ["application/json"],
                "x-api-key": [f"{ecommerce_api_key}"],
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "step": 0,
                "productId": 2,
                "data": {
                    "companyValidation": {
                        "BciFilterValidateCompanyURL": JsonPath.format(
                            "https://{}/onboarding/{}/{}/BciApiFilters/ValidCompanyApiFilters.json",
                            bucket_storage.bucket_name,
                            JsonPath.string_at(
                                "$.resultLegalEntities.body.EntidadLegal.Rut"
                            ),
                            JsonPath.string_at("$.processId"),
                        )
                    }
                },
            }
        ),
        result_path=JsonPath.DISCARD,
    )
    # Save Ecommerce
    save_ecommerce_act_no_permitidas = (
        aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
            scope,
            "Actualizar estado solicitud a RechazadaActividadesOGiroNoPermitidos",
            api=api_ecommerce,
            stage_name=os.environ.get("STAGE"),
            method=aws_stepfunctions_tasks.HttpMethod.PATCH,
            api_path="/partial-checkout",
            headers=TaskInput.from_object(
                {
                    "Content-Type": ["application/json"],
                    "x-api-key": [f"{ecommerce_api_key}"],
                    "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                    "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
                }
            ),
            request_body=TaskInput.from_object(
                {
                    "step": 0,
                    "productId": 2,
                    "data": {
                        "status": "RechazadaActividadesOGiroNoPermitidos",
                        "bciValidationProcessId": JsonPath.string_at("$.processId"),
                    },
                }
            ),
            result_path=JsonPath.DISCARD,
        )
    )
    save_ecommerce_company_notFound = (
        aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
            scope,
            "Actualizar estado solicitud a RechazadaEntidadNoEncontrada",
            api=api_ecommerce,
            stage_name=os.environ.get("STAGE"),
            method=aws_stepfunctions_tasks.HttpMethod.PATCH,
            api_path="/partial-checkout",
            headers=TaskInput.from_object(
                {
                    "Content-Type": ["application/json"],
                    "x-api-key": [f"{ecommerce_api_key}"],
                    "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                    "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
                }
            ),
            request_body=TaskInput.from_object(
                {
                    "step": 0,
                    "productId": 2,
                    "data": {
                        "status": "RechazadaEntidadNoEncontrada",
                        "bciValidationProcessId": JsonPath.string_at("$.processId"),
                    },
                }
            ),
            result_path=JsonPath.DISCARD,
        )
    )
    save_ecommerce_init_act = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Actualizar estado solicitud a RechazadaNoInicioActividades",
        api=api_ecommerce,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.PATCH,
        api_path="/partial-checkout",
        headers=TaskInput.from_object(
            {
                "Content-Type": ["application/json"],
                "x-api-key": [f"{ecommerce_api_key}"],
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "step": 0,
                "productId": 2,
                "data": {
                    "status": "RechazadaNoInicioActividades",
                    "bciValidationProcessId": JsonPath.string_at("$.processId"),
                },
            }
        ),
        result_path=JsonPath.DISCARD,
    )
    save_ecommerce_turn_end_date = (
        aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
            scope,
            "Actualizar estado solicitud a RechazadaTerminoActividades",
            api=api_ecommerce,
            stage_name=os.environ.get("STAGE"),
            method=aws_stepfunctions_tasks.HttpMethod.PATCH,
            api_path="/partial-checkout",
            headers=TaskInput.from_object(
                {
                    "Content-Type": ["application/json"],
                    "x-api-key": [f"{ecommerce_api_key}"],
                    "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                    "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
                }
            ),
            request_body=TaskInput.from_object(
                {
                    "step": 0,
                    "productId": 2,
                    "data": {
                        "status": "RechazadaTerminoActividades",
                        "bciValidationProcessId": JsonPath.string_at("$.processId"),
                    },
                }
            ),
            result_path=JsonPath.DISCARD,
        )
    )
    save_ecommerce_soc_type = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Actualizar estado solicitud a RechazadaTipoSociedad",
        api=api_ecommerce,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.PATCH,
        api_path="/partial-checkout",
        headers=TaskInput.from_object(
            {
                "Content-Type": ["application/json"],
                "x-api-key": [f"{ecommerce_api_key}"],
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "step": 0,
                "productId": 2,
                "data": {
                    "status": "RechazadaTipoSociedad",
                    "bciValidationProcessId": JsonPath.string_at("$.processId"),
                },
            }
        ),
        result_path=JsonPath.DISCARD,
    )
    save_ecommerce_filters_bci = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Actualizar estado solicitud a RechazadaFiltroBciEmpresa",
        api=api_ecommerce,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.PATCH,
        api_path="/partial-checkout",
        headers=TaskInput.from_object(
            {
                "Content-Type": ["application/json"],
                "x-api-key": [f"{ecommerce_api_key}"],
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "step": 0,
                "productId": 2,
                "data": {
                    "status": "RechazadaFiltroBciEmpresa",
                    "bciValidationProcessId": JsonPath.string_at("$.processId"),
                },
            }
        ),
        result_path="$.resultEcommerceFiltersBci",
    )
    save_ecommerce_error_interno = (
        aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
            scope,
            "Actualizar estado solicitud a ErrorInterno ",
            api=api_ecommerce,
            stage_name=os.environ.get("STAGE"),
            method=aws_stepfunctions_tasks.HttpMethod.PATCH,
            api_path="/partial-checkout",
            headers=TaskInput.from_object(
                {
                    "Content-Type": ["application/json"],
                    "x-api-key": [f"{ecommerce_api_key}"],
                    "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                    "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
                }
            ),
            request_body=TaskInput.from_object(
                {
                    "step": 0,
                    "productId": 2,
                    "data": {
                        "status": "ErrorInterno",
                        "bciValidationProcessId": JsonPath.string_at("$.processId"),
                    },
                }
            ),
            result_path=JsonPath.DISCARD,
        )
    )
    save_ecommerce_error_api_client = (
        aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
            scope,
            "Actualizar estado solicitud a ErrorApiCliente",
            api=api_ecommerce,
            stage_name=os.environ.get("STAGE"),
            method=aws_stepfunctions_tasks.HttpMethod.PATCH,
            api_path="/partial-checkout",
            headers=TaskInput.from_object(
                {
                    "Content-Type": ["application/json"],
                    "x-api-key": [f"{ecommerce_api_key}"],
                    "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                    "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
                }
            ),
            request_body=TaskInput.from_object(
                {
                    "step": 0,
                    "productId": 2,
                    "data": {
                        "status": "ErrorApiCliente",
                        "bciValidationProcessId": JsonPath.string_at("$.processId"),
                    },
                }
            ),
            result_path="$.resultEcommerceErrorApiClient",
        )
    )
    # Send Notify
    notify_verify_company = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Notificar Entidad No Encontrada",
        api=api_notify,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.POST,
        api_path="/notify-manager-app",
        auth_type=aws_stepfunctions_tasks.AuthType.IAM_ROLE,
        headers=TaskInput.from_object(
            {
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "query": "mutation notify ($id: String!, $status: String, $data: AWSJSON) {\
                    notify (id: $id, status: $status, data: $data) {\
                        id\
                        status\
                        data\
                    }\
                }",
                "variables": {
                    "id": JsonPath.string_at("$.processId"),
                    "status": "RechazadaEntidadNoEncontrada",
                },
            }
        ),
        result_path="$.resultNotifyCompanyNotFound",
    )
    notify_verify_init_act = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Notificar Entidad rechazada por no tener inicio actividades",
        api=api_notify,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.POST,
        api_path="/notify-manager-app",
        auth_type=aws_stepfunctions_tasks.AuthType.IAM_ROLE,
        headers=TaskInput.from_object(
            {
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        #
        request_body=TaskInput.from_object(
            {
                "query": "mutation notify ($id: String!, $status: String, $data: AWSJSON) {\
                    notify (id: $id, status: $status, data: $data) {\
                        id\
                        status\
                        data\
                    }\
                }",
                "variables": {
                    "id": JsonPath.string_at("$.processId"),
                    "status": "RechazadaNoInicioActividades",
                },
            }
        ),
        result_path="$.resultNotifyStartAct",
    )
    notify_verify_turn_end_date = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Notificar Entidad rechazada por tener fecha de termino de actividades",
        api=api_notify,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.POST,
        api_path="/notify-manager-app",
        auth_type=aws_stepfunctions_tasks.AuthType.IAM_ROLE,
        headers=TaskInput.from_object(
            {
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "query": "mutation notify ($id: String!, $status: String, $data: AWSJSON) {\
                    notify (id: $id, status: $status, data: $data) {\
                        id\
                        status\
                        data\
                    }\
                }",
                "variables": {
                    "id": JsonPath.string_at("$.processId"),
                    "status": "RechazadaTerminoActividades",
                },
            }
        ),
        result_path="$.resultNotifyEndAct",
    )
    notify_verify_economic_activity = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Notificar Entidad rechazada por actividades no permitidas",
        api=api_notify,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.POST,
        api_path="/notify-manager-app",
        auth_type=aws_stepfunctions_tasks.AuthType.IAM_ROLE,
        headers=TaskInput.from_object(
            {
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "query": "mutation notify ($id: String!, $status: String, $data: AWSJSON) {\
                    notify (id: $id, status: $status, data: $data) {\
                        id\
                        status\
                        data\
                    }\
                }",
                "variables": {
                    "id": JsonPath.string_at("$.processId"),
                    "status": "RechazadaActividadesOGiroNoPermitidos",
                },
            }
        ),
        result_path="$.resultNotifyNotAct",
    )
    notify_verify_soc_type = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Notificar Entidad rechazada por tipo de sociedad",
        api=api_notify,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.POST,
        api_path="/notify-manager-app",
        auth_type=aws_stepfunctions_tasks.AuthType.IAM_ROLE,
        headers=TaskInput.from_object(
            {
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "query": "mutation notify ($id: String!, $status: String, $data: AWSJSON) {\
                    notify (id: $id, status: $status, data: $data) {\
                        id\
                        status\
                        data\
                    }\
                }",
                "variables": {
                    "id": JsonPath.string_at("$.processId"),
                    "status": "RechazadaTipoSociedad",
                },
            }
        ),
        result_path="$.resultNotifySocType",
    )
    notify_verify_filters_bci = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Notificar Entidad rechazada por Api de filtros de BCI",
        api=api_notify,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.POST,
        api_path="/notify-manager-app",
        auth_type=aws_stepfunctions_tasks.AuthType.IAM_ROLE,
        headers=TaskInput.from_object(
            {
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "query": "mutation notify ($id: String!, $status: String, $data: AWSJSON) {\
                    notify (id: $id, status: $status, data: $data) {\
                        id\
                        status\
                        data\
                    }\
                }",
                "variables": {
                    "id": JsonPath.string_at("$.processId"),
                    "status": "RechazadaFiltroBciEmpresa",
                },
            }
        ),
        result_path="$.resultNotifyFiltersBci",
    )
    notify_valid_company = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Notificar Empresa Valida",
        api=api_notify,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.POST,
        api_path="/notify-manager-app",
        auth_type=aws_stepfunctions_tasks.AuthType.IAM_ROLE,
        headers=TaskInput.from_object(
            {
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "query": "mutation notify ($id: String!, $status: String, $data: AWSJSON) {\
                    notify (id: $id, status: $status, data: $data) {\
                        id\
                        status\
                        data\
                    }\
                }",
                "variables": {
                    "id": JsonPath.string_at("$.processId"),
                    "status": "EmpresaValida",
                },
            }
        ),
        result_path="$.resultNotifyValidCompany",
    )
    notify_error_interno_branch_1 = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Notificar Error Interno Branch 1",
        api=api_notify,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.POST,
        api_path="/notify-manager-app",
        auth_type=aws_stepfunctions_tasks.AuthType.IAM_ROLE,
        headers=TaskInput.from_object(
            {
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "query": "mutation notify ($id: String!, $status: String, $data: AWSJSON) {\
                    notify (id: $id, status: $status, data: $data) {\
                        id\
                        status\
                        data\
                    }\
                }",
                "variables": {
                    "id": JsonPath.string_at("$.processId"),
                    "status": "ErrorInterno",
                },
            }
        ),
        result_path="$.resultNotifyErrorInterno",
    )
    notify_error_api_client = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Notificar Error Api Cliente",
        api=api_notify,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.POST,
        api_path="/notify-manager-app",
        auth_type=aws_stepfunctions_tasks.AuthType.IAM_ROLE,
        headers=TaskInput.from_object(
            {
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "query": "mutation notify ($id: String!, $status: String, $data: AWSJSON) {\
                    notify (id: $id, status: $status, data: $data) {\
                        id\
                        status\
                        data\
                    }\
                }",
                "variables": {
                    "id": JsonPath.string_at("$.processId"),
                    "status": "ErrorApiCliente",
                },
            }
        ),
        result_path="$.resultNotifyErrorApiCliente",
    )

    fail_api_ecommerce = Fail(
        scope, "Fallo llamada Api Ecommerce",
        error="ErrorInterno", 
        cause="Falló la llamada a la API de Ecommerce"
    )

    fail_api_bci = Succeed(
        scope,
        "Solicitud rechazada (RechazadaFiltroBciEmpresa)",
    )
    fail_search_bussiness = Succeed(
        scope,
        "Solicitud rechazada (RechazadaEntidadNoEcontrada)"
    )
    branch_legalentities_bci_ok = Pass(
        scope,
        "Setear Branch LegalEntities-FiltrosBCI OK",
        parameters={"Branch_OK": True},
        result_path="$.BranchLegalEntities_BCI"
    )
    fail_error_interno = Fail(scope, "ErrorInterno")
    fail_error_api = Succeed(scope, "Solicitud rechazada (ErrorApiCliente)")

    filter_api = Choice(scope, "Validacion empresa con Api de filtros ?")
    filter_api.when(
        Condition.or_(
            Condition.is_present("$.resultBciApi.body.Error"),
            Condition.is_null("$.resultBciApi.body")
        ),
        save_ecommerce_error_api_client.add_catch(
            catch_save_ecommerce_branch_1, result_path=JsonPath.DISCARD),
    )
    filter_api.when(
        Condition.boolean_equals("$.resultBciApi.body.Data.Valido", True),
        branch_legalentities_bci_ok.next(Succeed(
            scope,
            "Exito en validacion de empresa con Legal Entity y Filtro BCI"
        )),
    )
    filter_api.when(
        Condition.boolean_equals("$.resultBciApi.body.Data.Valido", False),
        save_ecommerce_filters_bci.add_catch(catch_save_ecommerce_branch_1, result_path=JsonPath.DISCARD).next(
            notify_verify_filters_bci).next(fail_api_bci),
    )

    verify_subType = Pass(
        scope,
        "Verificar Tipo de sociedad",
        parameters={
            "ValidSubType.$": "States.ArrayContains($.Arrays.ArraySubType, $.resultLegalEntities.body.EntidadLegal.DatosAdicionales.SubTipoContribuyente)"
        },
        result_path="$.resultValidSubType",
    )

    verify_su = Choice(scope, "Validar Tipo de sociedad")
    verify_su.when(
        Condition.boolean_equals("$.resultValidSubType.ValidSubType", True),
        invoke_bci_api.add_retry(
            backoff_rate=1, max_attempts=3, interval=Duration.seconds(1)
        )
        .add_catch(
            Pass(scope, "Notificar error Api Filtros de BCI", result_path=JsonPath.DISCARD).next(save_ecommerce_error_api_client).next(notify_error_api_client).next(
                fail_error_api
            ),
            result_path=JsonPath.DISCARD
        )
        .next(
            save_bci_filters.add_retry(
                backoff_rate=1, max_attempts=3, interval=Duration.seconds(1)
            ).add_catch(save_ecommerce_error_interno, result_path=JsonPath.DISCARD)
        )
        .next(
            save_ecommerce_bci_filters.add_retry(
                backoff_rate=1, max_attempts=3, interval=Duration.seconds(1)
            ).add_catch(save_ecommerce_error_interno, result_path=JsonPath.DISCARD)
        )
        .next(filter_api),
    )
    verify_su.otherwise(
        save_ecommerce_soc_type.add_catch(catch_save_ecommerce_branch_1, result_path=JsonPath.DISCARD).next(notify_verify_soc_type).next(
            Succeed(
                scope,
                "Solicitud rechazada (RechazadaTipoSociedad)",
            )
        )
    )

    verify_sub = Choice(scope, "Contiene SubTipoContribuyente")
    verify_sub.when(
        Condition.is_not_null(
            "$.resultLegalEntities.body.EntidadLegal.DatosAdicionales"
        ),
        verify_subType.next(verify_su),
    )
    verify_sub.otherwise(invoke_bci_api)

    inject_array_act = Pass(
        scope,
        "Inyectar lista actividades no permitidas",
        parameters={
            "CodeActivities": [
                842300,
                829120,
                563001,
                551002,
                661209,
                641990,
                383001,
                663091,
                949903,
                661903,
                910100,
                889000,
                920090,
                466902,
                932901,
                843090,
                661904,
                643000,
                383009,
                17000,
                949904,
                661909,
                649100,
                651100,
                949909,
                841100,
                663092,
                663099,
                661902,
                949901,
                949902,
                990000,
                649209,
                649900,
                649202,
                649201,
                661204,
                942000,
                661100,
                842100,
                949100,
                451001,
                451002,
                454001
            ],
        },
        result_path="$.Arrays",
    )

    inject_array_subType = Pass(
        scope,
        "Inyectar lista tipos de sociedades",
        parameters={
            "ArraySubType": [
                "EMPR. INDIVIDUAL RESP. LTDA.",
                "SOC. RESPONSABILIDAD LIMITADA",
                "SOCIEDAD POR ACCIONES",
            ]
        },
        result_path="$.Arrays",
    )

    map_economic_activity = Map(
        scope,
        "Validar actividades economicas permitidas",
        items_path="$.resultLegalEntities.body.EntidadLegal.DatosBase.ActEconomicas",
        result_path="$.resultEconomicAct",
    )
    map_economic_activity.iterator(
        inject_array_act.next(
            Pass(
                scope,
                "Verificar actividad Economica",
                parameters={
                    "InvalidEconomicActiv.$": "States.ArrayContains($.Arrays.CodeActivities, $.Codigo)"
                },
                output_path="$.InvalidEconomicActiv",
            )
        )
    )

    verify_economic_activity = Choice(
        scope, "Realiza actividad economica no permitida ?"
    )
    verify_economic_activity.when(
        Condition.boolean_equals(
            "$.activiadadValida.invalidEconomicActiv", True),
        save_ecommerce_act_no_permitidas.add_catch(catch_save_ecommerce_branch_1, result_path=JsonPath.DISCARD).next(notify_verify_economic_activity).next(
            Succeed(
                scope, "Solicitud rechazada (RechazadaActividadesOGiroNoPermitidos)")
        ),
    )
    verify_economic_activity.otherwise(inject_array_subType.next(verify_sub))

    verify_if_contains_activity = Pass(
        scope,
        "Verificar si tuvo alguna actividad prohibida",
        parameters={
            "invalidEconomicActiv.$": "States.ArrayContains($.resultEconomicAct, true)"
        },
        result_path="$.activiadadValida",
    ).next(verify_economic_activity)
    
    calculate_array_activities_length = Pass(
        scope,
        "Calcular longitud de ActEconomicas",
        parameters={
            "length.$": "States.ArrayLength($.resultLegalEntities.body.EntidadLegal.DatosBase.ActEconomicas)"
        },
        result_path="$.actEconomicas"
    )
    
    verify_info_activities = Choice(scope, "Actividades económicas NULL o Vacías?")
    verify_info_activities.when(
        Condition.or_(
            Condition.is_null("$.resultLegalEntities.body.EntidadLegal.DatosBase.ActEconomicas"),
            Condition.is_not_present("$.resultLegalEntities.body.EntidadLegal.DatosBase.ActEconomicas"),
            Condition.and_(
                Condition.is_present("$.resultLegalEntities.body.EntidadLegal.DatosBase.ActEconomicas"),
                Condition.number_equals("$.actEconomicas.length", 0)
            )
        ),
        save_ecommerce_error_interno
    )
    verify_info_activities.otherwise(
        map_economic_activity.next(verify_if_contains_activity)
    )

    verify_giro = Choice(scope, "No tiene fecha de termino ?")
    verify_giro.when(
        Condition.or_(
            Condition.is_null(
                "$.resultLegalEntities.body.EntidadLegal.DatosAdicionales"
            ),
            Condition.and_(
                Condition.is_not_null(
                    "$.resultLegalEntities.body.EntidadLegal.DatosAdicionales"
                ),
                Condition.is_null(
                    "$.resultLegalEntities.body.EntidadLegal.DatosAdicionales.FchTerminoGiro"
                )
            )
        ),
        calculate_array_activities_length.next(verify_info_activities)
            
    )
    verify_giro.when(
        Condition.and_(
            Condition.is_not_null(
                "$.resultLegalEntities.body.EntidadLegal.DatosAdicionales"
            ),
            Condition.is_not_null(
                "$.resultLegalEntities.body.EntidadLegal.DatosAdicionales.FchTerminoGiro"
            ),
        ),
        save_ecommerce_turn_end_date.add_catch(catch_save_ecommerce_branch_1, result_path=JsonPath.DISCARD).next(notify_verify_turn_end_date).next(
            Succeed(
                scope,
                "Solicitud rechazada (RechazadaTerminoActividades)"
            )
        )
    )

    verify_act = Choice(scope, "Tiene Fecha de inicio actividades ?")
    verify_act.when(
        Condition.is_not_null(
            "$.resultLegalEntities.body.EntidadLegal.DatosBase.FchInicioActividades"
        ),
        verify_giro,
    )
    verify_act.otherwise(
        save_ecommerce_init_act.add_catch(catch_save_ecommerce_branch_1, result_path=JsonPath.DISCARD).next(notify_verify_init_act).next(
            Succeed(
                scope,
                "Solicitud rechazada (RechazadaNoInicioActividades)"
            )
        )
    )

    verify_bussines_exist = Choice(scope, "Empresa valida ?")
    verify_bussines_exist.when(
        Condition.and_(
            Condition.number_equals("$.resultLegalEntities.body.Codigo", 0),
            Condition.string_equals(
                "$.resultLegalEntities.body.Estado", "Completado")
        ),
        save_legal_entities.add_retry(
            backoff_rate=1, max_attempts=3, interval=Duration.seconds(1)
        )
        .add_catch(save_ecommerce_error_interno, result_path=JsonPath.DISCARD)
        .next(
            save_ecommerce.add_retry(
                backoff_rate=1, max_attempts=3, interval=Duration.seconds(1)
            ).add_catch(
                save_ecommerce_error_interno.add_catch(catch_save_ecommerce_branch_1, result_path=JsonPath.DISCARD).next(Pass(scope, "Notificar Error interno Ecommerce", result_path=JsonPath.DISCARD)).next(notify_error_interno_branch_1).next(
                    fail_error_interno
                ),
                result_path=JsonPath.DISCARD
            )
        )
        .next(verify_act),
    )
    verify_bussines_exist.when(
        Condition.and_(
            Condition.number_equals("$.resultLegalEntities.body.Codigo", 0),
            Condition.string_equals(
                "$.resultLegalEntities.body.Estado", "ObteniendoDatos")
        ),
        save_ecommerce_error_interno
    )
    verify_bussines_exist.otherwise(
        save_ecommerce_company_notFound.add_catch(catch_save_ecommerce_branch_1, result_path=JsonPath.DISCARD).next(notify_verify_company).next(
            fail_search_bussiness
        )
    )

    branch_legalentities_bci = (
        invoke_step_function.add_retry(
            backoff_rate=1, max_attempts=3, interval=Duration.seconds(1)
        )
        .add_catch(save_ecommerce_error_interno, result_path=JsonPath.DISCARD)
        .next(verify_bussines_exist)
    )

    state_json = {
        "Retry": [
            {
                "ErrorEquals": [
                    "States.ALL"
                ],
                "IntervalSeconds": 1,
                "MaxAttempts": 3,
                "BackoffRate": 1
            }
        ],
        "Catch": [
            {
                "ErrorEquals": [
                    "States.ALL"
                ],
                "ResultPath": "$.catchLegalbot",
                "Next": "Notificar error api Legalbot"
            }
        ],
        "Type": "Task",
        "ResultPath": "$.resultLegalBot",
        "ResultSelector": {
            "body.$": "$.ResponseBody"
        },
        "Resource": "arn:aws:states:::apigateway:invoke",
        "Parameters": {
            "ApiEndpoint": f"{lendbot_api_id}.execute-api.{os.environ.get('LENDBOT_REGION')}.amazonaws.com",
            "Method": "GET",
            "Headers": {
                "Content-Type": [
                    "application/json"
                ],
                "x-api-key": [
                    f"{lendbot_api_key}"
                ],
                "service-api-key": [
                    f"{os.environ.get('LEGALBOT_KEY')}"
                ]
            },
            "Stage": f"{os.environ.get('STAGE')}",
            "Path.$": "States.Format('/legalbot/v1/studies/DATAMART/{}', $.rut)",
            "QueryParameters": {
                "update-data": [
                    "true"
                ]
            },
            "AuthType": "NO_AUTH"
        }
    }

    invoke_api_legal_bot = CustomState(
        scope, "Consulta LegalBot",
        state_json=state_json
    )

    get_legal_bot_json = aws_stepfunctions_tasks.LambdaInvoke(
        scope,
        "Procesar respuesta de Legalbot",
        lambda_function=lambda_get_legalbot,
        payload=TaskInput.from_json_path_at("$.resultLegalBot.body"),
        result_selector={"body": JsonPath.string_at("$.Payload.body")},
        result_path="$.legalBot",
    )
    
    check_duration = aws_stepfunctions_tasks.LambdaInvoke(
        scope, "Calcular y validar duracion empresa definida",
        lambda_function=lambda_check_duration,
        payload=TaskInput.from_object({
            "durationEndDate": JsonPath.string_at("$.legalBot.body.durationEndDate")
        }),
        result_path="$.Duration"
    )

    save_error_interno = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Actualizar solicitud a ErrorInterno",
        api=api_ecommerce,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.PATCH,
        api_path="/partial-checkout",
        headers=TaskInput.from_object(
            {
                "Content-Type": ["application/json"],
                "x-api-key": [f"{ecommerce_api_key}"],
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "step": 0,
                "productId": 2,
                "data": {
                    "status": "ErrorInterno",
                },
            }
        ),
        result_path=JsonPath.DISCARD,
    )

    save_legalbot_response = aws_stepfunctions_tasks.CallAwsService(
        scope,
        "Guardar Legalbot en S3",
        service="s3",
        action="putObject",
        iam_action="s3:PutObject",
        iam_resources=[f"{bucket_storage.bucket_arn}/*"],
        parameters={
            "Body": JsonPath.string_at("$.legalBot.body"),
            "Bucket": bucket_storage.bucket_name,
            "Key": JsonPath.format(
                "onboarding/{}/{}/legalbot/legalbot_response.json",
                JsonPath.string_at("$.rut"),
                JsonPath.string_at("$.processId"),
            ),
            "ContentType": "application/json",
        },
        result_path=JsonPath.DISCARD,
    ).add_retry(
        backoff_rate=1,
        max_attempts=3,
        interval=Duration.seconds(1)
    ).add_catch(save_error_interno, result_path=JsonPath.DISCARD)

    save_legalbot_response_no_res = aws_stepfunctions_tasks.CallAwsService(
        scope,
        "Guardar Legalbot -No es empresa en un dia- en S3",
        service="s3",
        action="putObject",
        iam_action="s3:PutObject",
        iam_resources=[f"{bucket_storage.bucket_arn}/*"],
        parameters={
            "Body": JsonPath.string_at("$.resultLegalBot.body"),
            "Bucket": bucket_storage.bucket_name,
            "Key": JsonPath.format(
                "onboarding/{}/{}/legalbot/legalbot_response.json",
                JsonPath.string_at("$.rut"),
                JsonPath.string_at("$.processId"),
            ),
            "ContentType": "application/json",
        },
        result_path=JsonPath.DISCARD,
    ).add_retry(
        backoff_rate=1,
        max_attempts=3,
        interval=Duration.seconds(1)
    ).add_catch(save_error_interno, result_path=JsonPath.DISCARD)

    save_ecommerce_legalbot_error = (
        aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
            scope,
            "Actualizar estado solicitud a RechazadaDeterminandoApoderadosSocios",
            api=api_ecommerce,
            stage_name=os.environ.get("STAGE"),
            method=aws_stepfunctions_tasks.HttpMethod.PATCH,
            api_path="/partial-checkout",
            headers=TaskInput.from_object(
                {
                    "Content-Type": ["application/json"],
                    "x-api-key": [f"{ecommerce_api_key}"],
                    "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                    "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
                }
            ),
            request_body=TaskInput.from_object(
                {
                    "step": 0,
                    "productId": 2,
                    "data": {
                        "status": "RechazadaDeterminandoApoderadosSocios",
                        "bciValidationProcessId": JsonPath.string_at("$.processId"),
                    },
                }
            ),
            result_path="$.resultEcommerceLegalBotError",
        )
    )
    save_legalbot_url_in_request = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Guardar antencedente legalbot en solicitud",
        api=api_ecommerce,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.PATCH,
        api_path="/partial-checkout",
        headers=TaskInput.from_object(
            {
                "Content-Type": ["application/json"],
                "x-api-key": [f"{ecommerce_api_key}"],
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "step": 0,
                "productId": 2,
                "data": {
                    "partnersAndAttorneysData": {
                        "legalbotFuenteURL": JsonPath.format(
                            "https://{}/onboarding/{}/{}/legalbot/legalbot_response.json",
                            bucket_storage.bucket_name,
                            JsonPath.string_at("$.rut"),
                            JsonPath.string_at("$.processId"),
                        )
                    }
                },
            }
        ),
        result_path=JsonPath.DISCARD,
    )

    save_legalbot_no_res = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Guardar legalbot cuando -No es empresa en un día- en solicitud",
        api=api_ecommerce,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.PATCH,
        api_path="/partial-checkout",
        headers=TaskInput.from_object(
            {
                "Content-Type": ["application/json"],
                "x-api-key": [f"{ecommerce_api_key}"],
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "step": 0,
                "productId": 2,
                "data": {
                    "partnersAndAttorneysData": {
                        "legalbotFuenteURL": JsonPath.format(
                            "https://{}/onboarding/{}/{}/legalbot/legalbot_response.json",
                            bucket_storage.bucket_name,
                            JsonPath.string_at("$.rut"),
                            JsonPath.string_at("$.processId"),
                        ),
                        "RegistroRES": JsonPath.string_at("$.RegistroRes.Existe")
                    }
                },
            }
        ),
        result_path=JsonPath.DISCARD,
    )

    invoke_step_function_bci_rest = aws_stepfunctions_tasks.StepFunctionsStartExecution(
        scope,
        "Iniciar RES WFL",
        state_machine=state_machine_bci_res,
        integration_pattern=IntegrationPattern.REQUEST_RESPONSE,
        input=TaskInput.from_object({"rut": JsonPath.string_at("$.rut")}),
        name=JsonPath.string_at("$.processId"),
        result_path="$.resultBCIRest",
    )
    
    save_ecommerce_rechazada_duration_definida = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Actualizar estado a RechazadaDuracionDefinida",
        api=api_ecommerce,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.PATCH,
        api_path="/partial-checkout",
        headers=TaskInput.from_object(
            {
                "Content-Type": ["application/json"],
                "x-api-key": [f"{ecommerce_api_key}"],
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "step": 0,
                "productId": 2,
                "data": {
                    "status": "RechazadaDuracionDefinida",
                    "bciValidationProcessId": JsonPath.string_at("$.processId")
                },
            }
        ),
        result_path=JsonPath.DISCARD
    )
    
    notify_rechazada_duration_definida = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Notificar solicitud RechazadaDuracionDefinida",
        api=api_notify,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.POST,
        api_path="/notify-manager-app",
        auth_type=aws_stepfunctions_tasks.AuthType.IAM_ROLE,
        headers=TaskInput.from_object(
            {
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "query": "mutation notify ($id: String!, $status: String, $data: AWSJSON) {\
                    notify (id: $id, status: $status, data: $data) {\
                        id\
                        status\
                        data\
                    }\
                }",
                "variables": {
                    "id": JsonPath.string_at("$.processId"),
                    "status": "RechazadaDuracionDefinida",
                },
            }
        ),
        result_path=JsonPath.DISCARD
    )
    
    
    save_ecommerce_rechazada_partner_pyme = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Actualizar estado a RechazadaSocioPyme",
        api=api_ecommerce,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.PATCH,
        api_path="/partial-checkout",
        headers=TaskInput.from_object(
            {
                "Content-Type": ["application/json"],
                "x-api-key": [f"{ecommerce_api_key}"],
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "step": 0,
                "productId": 2,
                "data": {
                    "status": "RechazadaSocioPyme",
                    "partnersAndAttorneysData": {
                        "partners.$": "$.legalBot.body.associates"
                    },
                    "bciValidationProcessId": JsonPath.string_at("$.processId")
                },
            }
        ),
        result_path=JsonPath.DISCARD
    )
    
    notify_rechazada_rechazada_partner_pyme = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Notificar solicitud RechazadaSocioPyme",
        api=api_notify,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.POST,
        api_path="/notify-manager-app",
        auth_type=aws_stepfunctions_tasks.AuthType.IAM_ROLE,
        headers=TaskInput.from_object(
            {
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "query": "mutation notify ($id: String!, $status: String, $data: AWSJSON) {\
                    notify (id: $id, status: $status, data: $data) {\
                        id\
                        status\
                        data\
                    }\
                }",
                "variables": {
                    "id": JsonPath.string_at("$.processId"),
                    "status": "RechazadaSocioPyme",
                },
            }
        ),
        result_path=JsonPath.DISCARD
    )

    save_ecommerce_rechazada_tipo_sociedad = (
        aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
            scope,
            "Actualizar estado a RechazadaTipoSociedad",
            api=api_ecommerce,
            stage_name=os.environ.get("STAGE"),
            method=aws_stepfunctions_tasks.HttpMethod.PATCH,
            api_path="/partial-checkout",
            headers=TaskInput.from_object(
                {
                    "Content-Type": ["application/json"],
                    "x-api-key": [f"{ecommerce_api_key}"],
                    "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                    "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
                }
            ),
            request_body=TaskInput.from_object(
                {
                    "step": 0,
                    "productId": 2,
                    "data": {
                        "status": "RechazadaTipoSociedad",
                        "bciValidationProcessId": JsonPath.string_at("$.processId")
                    },
                }
            ),
            result_path="$.save_ecommerce_tipo_sociedad_companyKind",
        )
    )

    save_ecommerce_tipo_cantidad_socios = (
        aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
            scope,
            "Actualizar estado a rechazado por cantidad de Socios",
            api=api_ecommerce,
            stage_name=os.environ.get("STAGE"),
            method=aws_stepfunctions_tasks.HttpMethod.PATCH,
            api_path="/partial-checkout",
            headers=TaskInput.from_object(
                {
                    "Content-Type": ["application/json"],
                    "x-api-key": [f"{ecommerce_api_key}"],
                    "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                    "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
                }
            ),
            request_body=TaskInput.from_object(
                {
                    "step": 0,
                    "productId": 2,
                    "data": {
                        "status": "RechazadaCantidadSocios",
                        "bciValidationProcessId": JsonPath.string_at("$.processId")
                    },
                }
            ),
            result_path="$.save_ecommerce_tipo_cantidad_socios",
        )
    )

    save_partners_LegalBot = (
        aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
            scope,
            "Guardar en solicitud datos de socios",
            api=api_ecommerce,
            stage_name=os.environ.get("STAGE"),
            method=aws_stepfunctions_tasks.HttpMethod.PATCH,
            api_path="/partial-checkout",
            headers=TaskInput.from_object(
                {
                    "Content-Type": ["application/json"],
                    "x-api-key": [f"{ecommerce_api_key}"],
                    "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                    "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
                }
            ),
            request_body=TaskInput.from_object(
                {
                    "step": 0,
                    "productId": 2,
                    "data": {
                        "partnersAndAttorneysData": {
                            "partners.$": "$.legalBot.body.associates",
                            "studyId.$": "$.legalBot.body.id",
                            "rut.$": "$.legalBot.body.rut",
                            "Dispatch.$": "$.ExecuteDispatch.Dispatch"
                        }
                    },
                }
            ),
            result_path="$.firmantes",
        )
    )

    fail_ApoderadosYSocios = Succeed(
        scope,
        "Solicitud rechazada (RechazadaDeterminandoApoderadosSocios)"
    )

    fail_RegistroRes = Fail(
        scope, "Fallo en rama Legalbot (ErrorInterno)"
    )

    fail_verificacion_tipo_sociedad_legal_bot = Succeed(
        scope,
        "Solicitud rechazada (RechazadaCantidadSocios)"
    )
    solicitud_Rechazada = Succeed(
        scope,
        "Solicitud rechazada (RechazadaNoRes)"
    )

    obtener_cant_associates = Pass(
        scope,
        "Obtener cantidad de socios",
        parameters={
            "length.$": "States.ArrayLength($.legalBot.body.associates)"},
        result_path="$.obtener_cant_associates",
    )
    branch_legalbot_ok = Pass(
        scope,
        "Setear Branch Legalbot OK",
        parameters={"Branch_OK": True},
        result_path="$.BranchLegalbot"
    )

    notify_no_res = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Notificar solicitud RechazadaNoRES",
        api=api_notify,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.POST,
        api_path="/notify-manager-app",
        auth_type=aws_stepfunctions_tasks.AuthType.IAM_ROLE,
        headers=TaskInput.from_object(
            {
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "query": "mutation notify ($id: String!, $status: String, $data: AWSJSON) {\
                    notify (id: $id, status: $status, data: $data) {\
                        id\
                        status\
                        data\
                    }\
                }",
                "variables": {
                    "id": JsonPath.string_at("$.processId"),
                    "status": "RechazadaNoRES",
                },
            }
        ),
        result_path="$.result_notify_no_res",
    )
    notify_error_interno_branch_0 = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Notificar Error Interno Branch 0",
        api=api_notify,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.POST,
        api_path="/notify-manager-app",
        auth_type=aws_stepfunctions_tasks.AuthType.IAM_ROLE,
        headers=TaskInput.from_object(
            {
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "query": "mutation notify ($id: String!, $status: String, $data: AWSJSON) {\
                    notify (id: $id, status: $status, data: $data) {\
                        id\
                        status\
                        data\
                    }\
                }",
                "variables": {
                    "id": JsonPath.string_at("$.processId"),
                    "status": "ErrorInterno",
                },
            }
        ),
        result_path="$.result_notify_error_interno",
    )
    notify_solicitud_rechazada_companyKind = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Notificar Rechazada Tipo Sociedad",
        api=api_notify,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.POST,
        api_path="/notify-manager-app",
        auth_type=aws_stepfunctions_tasks.AuthType.IAM_ROLE,
        headers=TaskInput.from_object(
            {
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "query": "mutation notify ($id: String!, $status: String, $data: AWSJSON) {\
                    notify (id: $id, status: $status, data: $data) {\
                        id\
                        status\
                        data\
                    }\
                }",
                "variables": {
                    "id": JsonPath.string_at("$.processId"),
                    "status": "RechazadaTipoSociedad",
                },
            }
        ),
        result_path="$.result_notify_solicitud_rechazada_companyKind",
    )

    result_notify_solicitud_rechazada_cantidad_socios = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Notificar RechazadaCantidadSocios",
        api=api_notify,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.POST,
        api_path="/notify-manager-app",
        auth_type=aws_stepfunctions_tasks.AuthType.IAM_ROLE,
        headers=TaskInput.from_object(
            {
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "query": "mutation notify ($id: String!, $status: String, $data: AWSJSON) {\
                    notify (id: $id, status: $status, data: $data) {\
                        id\
                        status\
                        data\
                    }\
                }",
                "variables": {
                    "id": JsonPath.string_at("$.processId"),
                    "status": "RechazadaCantidadSocios",
                },
            }
        ),
        result_path="$.result_notify_solicitud_rechazada_cantidad_socios",
    )

    notify_filter_Legalbot = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Notificar Filtro Legal Bot Completado",
        api=api_notify,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.POST,
        api_path="/notify-manager-app",
        auth_type=aws_stepfunctions_tasks.AuthType.IAM_ROLE,
        headers=TaskInput.from_object(
            {
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "query": "mutation notify ($id: String!, $status: String, $data: AWSJSON) {\
                    notify (id: $id, status: $status, data: $data) {\
                        id\
                        status\
                        data\
                    }\
                }",
                "variables": {
                    "id": JsonPath.string_at("$.processId"),
                    "status": "ApoderadosYSociosRecuperados",
                },
            }
        ),
        result_path="$.result_Notify_Filtre_LegalBot",
    )
    notify_rechazo_determinando_apoder_socios = aws_stepfunctions_tasks.CallApiGatewayRestApiEndpoint(
        scope,
        "Notificar Rechazada determinando Apoderados y Socios",
        api=api_notify,
        stage_name=os.environ.get("STAGE"),
        method=aws_stepfunctions_tasks.HttpMethod.POST,
        api_path="/notify-manager-app",
        auth_type=aws_stepfunctions_tasks.AuthType.IAM_ROLE,
        headers=TaskInput.from_object(
            {
                "cookie": JsonPath.array(JsonPath.string_at("$.cookie")),
                "x-csrftoken": JsonPath.array(JsonPath.string_at("$.csrftoken")),
            }
        ),
        request_body=TaskInput.from_object(
            {
                "query": "mutation notify ($id: String!, $status: String, $data: AWSJSON) {\
                    notify (id: $id, status: $status, data: $data) {\
                        id\
                        status\
                        data\
                    }\
                }",
                "variables": {
                    "id": JsonPath.string_at("$.processId"),
                    "status": "RechazadaDeterminandoApoderadosSocios",
                },
            }
        ),
        result_path="$.resultNotifyRechazadaDeterminandoApoderadosSocios",
    )

    validar_company_and_associates = (
        Choice(scope, "Validar cantidad socios por tipo de sociedad")
        .when(
            Condition.or_(
                Condition.and_(
                    Condition.or_(
                        Condition.number_equals(
                            "$.legalBot.body.companyKind", 1),
                    ),
                    Condition.and_(
                        Condition.number_greater_than_equals(
                            "$.obtener_cant_associates.length", 1),
                        Condition.number_less_than_equals(
                            "$.obtener_cant_associates.length", 5)
                    )
                ),
                Condition.and_(
                    Condition.or_(
                        Condition.number_equals(
                            "$.legalBot.body.companyKind", 2),
                    ),
                    Condition.number_equals(
                        "$.obtener_cant_associates.length", 1)
                ),
                Condition.and_(
                    Condition.or_(
                        Condition.number_equals(
                            "$.legalBot.body.companyKind", 3),
                    ),
                    Condition.and_(
                        Condition.number_greater_than_equals(
                            "$.obtener_cant_associates.length", 1),
                        Condition.number_less_than_equals(
                            "$.obtener_cant_associates.length", 5)
                    )
                )
            ),
            save_partners_LegalBot.add_catch(catch_save_ecommerce_branch_0, result_path=JsonPath.DISCARD).next(
                notify_filter_Legalbot.next(branch_legalbot_ok).next(
                    Succeed(scope, "Completado Legalbot"))
            )
        )
        .otherwise(
            save_ecommerce_tipo_cantidad_socios.add_catch(catch_save_ecommerce_branch_0, result_path=JsonPath.DISCARD).next(
                result_notify_solicitud_rechazada_cantidad_socios
            ).next(fail_verificacion_tipo_sociedad_legal_bot)
        )
    )
    
    extraer_parte_numerica = Pass(
        scope, "Extraer parte numerica",
        parameters={
            "rut_int.$": "States.StringSplit($.rut, '-')"
        },
        output_path="$.rut_int"
    )
    
    convertir_rut_a_numero = Pass(
        scope, "ConvertirRutANumero",
        parameters={
            "rut_num.$": "States.StringToJson($[0])"
        }
    )
    
    verificar_rut_value = Choice(scope, "CheckRutValue")
    verificar_rut_value.when(
        Condition.number_greater_than("$.rut_num", 50000000),
        Fail(scope, "RechazarSolicitud", error="RechazadaSocioPyme", cause="Un socio tiene el RUT clasificado como Pyme")
    ).otherwise(Succeed(scope, "Continuar"))

    verificar_ruts = Map(
        scope, "Verificar Ruts socios",
        items_path="$.resultProcesarRuts",
        result_path="$.resultVerificarRuts"
    )
    verificar_ruts.iterator(convertir_rut_a_numero.next(verificar_rut_value))
    verificar_ruts.add_catch(
        save_ecommerce_rechazada_partner_pyme.add_catch(
            catch_save_ecommerce_branch_0, result_path=JsonPath.DISCARD).next(
                notify_rechazada_rechazada_partner_pyme).next(
                    Succeed(
                        scope,
                        "Empresa Rechazada (RechazadaSocioPyme)",
                        comment="Un socio tiene el RUT clasificado como Pyme"
                    )
                ),
                result_path="$.resultCatchPartner"
            )
    
    map_state_procesar_ruts = Map(
        scope, "Procesar Ruts socios",
        items_path="$.legalBot.body.associates",
        result_path="$.resultProcesarRuts"
    )
    map_state_procesar_ruts.iterator(extraer_parte_numerica)

    partner_process = map_state_procesar_ruts.next(
        verificar_ruts).next(
            obtener_cant_associates).next(
                validar_company_and_associates)

    error_interno = save_error_interno.add_catch(catch_save_ecommerce_branch_0, result_path=JsonPath.DISCARD).next(notify_error_interno_branch_0).next(
        fail_RegistroRes
    )

    pass_sns = Pass(scope, "Notificar error api Legalbot",
                    result_path=JsonPath.DISCARD).next(error_interno)

    validar_companyKind = (
        Choice(scope, "Validar tipo de sociedad Legalbot")
        .when(
            Condition.or_(
                Condition.number_equals("$.legalBot.body.companyKind", 1),
                Condition.number_equals("$.legalBot.body.companyKind", 2),
                Condition.number_equals("$.legalBot.body.companyKind", 3),
            ),
            partner_process
        )
        .otherwise(
            save_ecommerce_rechazada_tipo_sociedad.add_catch(catch_save_ecommerce_branch_0, result_path=JsonPath.DISCARD).next(
                notify_solicitud_rechazada_companyKind
            ).next(
                Succeed(
                    scope,
                    "Empresa Rechazada (RechazadaTipoSociedad)",
                )
            )
        )
    )
    
    dispatch_true = Pass(
        scope, "Setear Dispatch a true",
        parameters={
            "Dispatch": True
        },
        result_path="$.ExecuteDispatch"
    )
    
    dispatch_false = Pass(
        scope, "Setear Dispatch a False",
        parameters={
            "Dispatch": False
        },
        result_path="$.ExecuteDispatch"
    )
    
    verify_duration_end_date = Choice(scope, "Resultado verificacion de duracion definida")
    verify_duration_end_date.when(
        Condition.boolean_equals("$.Duration.Payload.valid", True),
        validar_companyKind  
    )
    verify_duration_end_date.otherwise(
        save_ecommerce_rechazada_duration_definida.next(
            notify_rechazada_duration_definida).next(
                Succeed(
                        scope,
                        "Empresa Rechazada (RechazadaDuracionDefinida)",
                    )
                )
    )
    
    verify_duration_business = Choice(scope, "Verificar Tipo duracion de empresa")
    verify_duration_business.when(
        Condition.and_(
            Condition.is_present("$.legalBot.body.durationType"),
            Condition.number_equals("$.legalBot.body.durationType", 7)
        ),
        check_duration.next(verify_duration_end_date)
    )
    verify_duration_business.otherwise(
        validar_companyKind
    )
    
    dispatch_duration = Choice(scope, "Contiene informacion de duracion ?")
    dispatch_duration.when(
        Condition.and_(
            Condition.is_not_present("$.legalBot.body.durationType"),
            Condition.or_(
                Condition.and_(
                    Condition.boolean_equals("$.legalBot.body.isApproved", True),
                    Condition.boolean_equals("$.legalBot.body.isPreApproved", False),
                ),
                Condition.and_(
                    Condition.is_null("$.legalBot.body.isApproved"),
                    Condition.boolean_equals("$.legalBot.body.isPreApproved", False),
                ) 
            )
        ),
        dispatch_true.next(save_partners_LegalBot)
    )
    dispatch_duration.otherwise(
        verify_duration_business
    )
    
    verify_aproval_business = Choice(scope, "Verificar aprobacion legal de empresa")
    verify_aproval_business.when(
        Condition.or_(
            Condition.and_(
                Condition.boolean_equals("$.legalBot.body.isApproved", True),
                Condition.boolean_equals("$.legalBot.body.isPreApproved", True),
            ),
            Condition.and_(
                Condition.boolean_equals("$.legalBot.body.isApproved", True),
                Condition.boolean_equals("$.legalBot.body.isPreApproved", False),
            )
        ),
        dispatch_false.next(dispatch_duration)
    )
    verify_aproval_business.when(
        Condition.and_(
            Condition.is_null("$.legalBot.body.isApproved"),
            Condition.boolean_equals("$.legalBot.body.isPreApproved", False),
        ),
        dispatch_true
    )
    verify_aproval_business.otherwise(
        save_ecommerce_legalbot_error.add_catch(catch_save_ecommerce_branch_0, result_path=JsonPath.DISCARD).next(
            notify_rechazo_determinando_apoder_socios
        ).next(fail_ApoderadosYSocios)
    )
        
    def_legalbot = get_legal_bot_json.next(
        save_legalbot_response).next(
            save_legalbot_url_in_request.add_catch(catch_save_ecommerce_branch_0, result_path=JsonPath.DISCARD)).next(
                verify_aproval_business
    )

    legal_bot_recuperado = Choice(scope, "Es empresa en un día ?")
    legal_bot_recuperado.when(
        Condition.and_(
            Condition.is_null("$.resultLegalBot.body.EnlaceJson"),
            Condition.number_equals("$.resultLegalBot.body.Codigo", 0),
            Condition.string_equals(
                "$.resultLegalBot.body.Mensaje", "El RUT solicitado no es de empresa en un día.")
        ),
        save_legalbot_response_no_res.next(
            Pass(
                scope, "Setear no existe en el RES",
                parameters={
                    "Existe": False
                },
                result_path="$.RegistroRes"
            )
        ).next(save_legalbot_no_res.add_catch(catch_save_ecommerce_branch_0, result_path=JsonPath.DISCARD)).next(notify_filter_Legalbot)
    )
    legal_bot_recuperado.when(
        Condition.and_(
            Condition.is_not_null("$.resultLegalBot.body.EnlaceJson"),
            Condition.number_equals("$.resultLegalBot.body.Codigo", 0),
            Condition.string_equals("$.resultLegalBot.body.Mensaje", "Descarga realizada.")
        ),
        def_legalbot
    )

    branch_legalbot_res = invoke_api_legal_bot.next(legal_bot_recuperado)

    parallel = Parallel(
        scope,
        "Validaciones de empresa",
        result_path="$.parallel",
        result_selector={
            "resultBranchLegalbot": JsonPath.string_at("$[0]"),
            "resultBranchEntitiesBCI": JsonPath.string_at("$[1]")
        }
    )

    parallel.branch(branch_legalbot_res)
    parallel.branch(branch_legalentities_bci)

    check_branch_result = Choice(scope, "Verificar si ocurrio Rechazo")
    check_branch_result.when(
        Condition.and_(
            Condition.is_present(
                '$.parallel.resultBranchLegalbot.BranchLegalbot.Branch_OK'),
            Condition.is_present(
                '$.parallel.resultBranchEntitiesBCI.BranchLegalEntities_BCI.Branch_OK')
        ),
        notify_valid_company.next(
            Choice(scope, "Empresa registrada en RES ?")
            .when(
                Condition.is_present(
                    '$.parallel.resultBranchLegalbot.RegistroRes'
                ),
                Succeed(scope, "Esta Empresa no esta en el RES")
            )
            .otherwise(invoke_step_function_bci_rest)
        )
    )
    check_branch_result.otherwise(Succeed(scope, "Esta Empresa fue rechazada"))

    definition = task_put_event.next(invoke_ecommerce_api).next(
        Choice(scope, "Datos de solicitud guardados ?")
        .when(Condition.is_present("$.resultEcommerceApi.StatusCode"),
              parallel.next(check_branch_result)
              )
        .otherwise(fail_api_ecommerce)
    )

    return StateMachine(
        scope,
        pascal_case(state_machine_name),
        state_machine_name=state_machine_name,
        state_machine_type=StateMachineType.STANDARD,
        definition=definition,
        role=role,
        logs=LogOptions(
            destination=log_group_sf, level=LogLevel.ALL, include_execution_data=True
        ),
        tracing_enabled=True,
    )
