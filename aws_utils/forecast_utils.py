from datetime import datetime
from gui.error_display_functions import error, worker_error_page


def list_all_forecast_regions(session):
    """List all regions with AWS Forecast"""
    try:
        regions = session.get_available_regions('forecast', partition_name='aws', allow_non_regional=False)
        ssm_client = session.client('ssm', region_name='us-east-1')
    except Exception as e:
        worker_error_page("Error!", str(e))
        return None, 'failure'

    regions_names = {}
    for resp in regions:
        region_id = resp
        path = '/aws/service/global-infrastructure/regions/%s/longName' % region_id
        ssm_response = ssm_client.get_parameter(Name=path)
        region_name = ssm_response['Parameter']['Value']
        regions_names[region_id] = region_name
    return regions_names, 'success'


def list_all_forecast_dataset_groups(forecast_client):
    """List all dataset groups inside AWS Forecast"""
    try:
        response = forecast_client.list_dataset_groups()
        dataset_groups = response['DatasetGroups']
        retail_dataset_groups = {}
        for dataset_group in dataset_groups:
            dataset_group_arn = dataset_group['DatasetGroupArn']
            dataset_response = forecast_client.describe_dataset_group(
                DatasetGroupArn=dataset_group_arn
            )
            if dataset_response['Domain'] == 'RETAIL':
                retail_dataset_groups[dataset_group['DatasetGroupName']] = dataset_group['DatasetGroupArn']
        return retail_dataset_groups
    except Exception as e:
        error(str(e))


def is_location_in_datasets(forecast_client, dataset_group_arn):
    """Checks if related any dataset has 'location' as a parameter"""
    try:
        response = forecast_client.describe_dataset_group(
            DatasetGroupArn=dataset_group_arn
        )
        datasets_arns = response['DatasetArns']
        for dataset_arn in datasets_arns:
            dataset_response = forecast_client.describe_dataset(
                DatasetArn=dataset_arn
            )
            schema = dataset_response['Schema']
            attributes = schema['Attributes']
            for attribute in attributes:
                if attribute['AttributeName'] == 'location':
                    return True, 'success'
        return False, 'success'
    except Exception as e:
        error(str(e))
        return False, 'failure'


def list_all_forecast_datasets(forecast_client, dataset_group_arn):
    """Lists all datasets inside a dataset group"""
    try:
        response = forecast_client.describe_dataset_group(
            DatasetGroupArn=dataset_group_arn
        )
    except Exception as e:
        error(str(e))

    datasets_arns = response['DatasetArns']
    dataset_arn_dict = {}
    dataset_type_dict = {}
    dataset_status = {}
    for dataset_arn in datasets_arns:
        dataset_response = forecast_client.describe_dataset(
            DatasetArn=dataset_arn
        )
        if dataset_response['Status'] == 'ACTIVE':
            dataset_arn_dict[dataset_response['DatasetName']] = dataset_response['DatasetArn']
            dataset_type_dict[dataset_response['DatasetName']] = dataset_response['DatasetType']
            dataset_status[dataset_response['DatasetName']] = 'ACTIVE'
        elif dataset_response['Status'] == 'UPDATE_IN_PROGRESS':
            dataset_arn_dict[dataset_response['DatasetName']] = dataset_response['DatasetArn']
            dataset_type_dict[dataset_response['DatasetName']] = dataset_response['DatasetType']
            dataset_status[dataset_response['DatasetName']] = 'CREATING'
    return dataset_arn_dict, dataset_type_dict, dataset_status


def list_all_forecast_predictors(forecast_client, dataset_group_arn):
    """Lists all predictors inside a dataset group"""

    response = forecast_client.list_predictors(
        Filters=[
            {
                'Key': 'DatasetGroupArn',
                'Value': dataset_group_arn,
                'Condition': 'IS'
            }
        ]
    )
    predictors_arn_dict = {}
    predictors = response['Predictors']
    for predictor in predictors:
        predictors_arn_dict[predictor['PredictorName']] = predictor['PredictorArn']
    return predictors_arn_dict


def list_all_forecast_forecasts(forecast_client, dataset_group_arn, s3_path):
    """List all forecasts inside a dataset group"""
    try:
        forecast_set = set()
        predictor_set = set()
        forecast_exports_set = set()
        response = forecast_client.list_forecasts(
            Filters=[
                {
                    'Key': 'DatasetGroupArn',
                    'Value': dataset_group_arn,
                    'Condition': 'IS'
                },
                {
                    'Key': 'Status',
                    'Value': 'ACTIVE',
                    'Condition': 'IS'
                }
            ]
        )
        forecasts_arn_dict = {}
        forecasts = response['Forecasts']
        for forecast in forecasts:
            forecast_set.add(forecast['ForecastName'])
            forecasts_arn_dict[forecast['ForecastName']] = forecast['ForecastArn']
        response = forecast_client.list_predictors(
            Filters=[
                {
                    'Key': 'DatasetGroupArn',
                    'Value': dataset_group_arn,
                    'Condition': 'IS'
                },
                {
                    'Key': 'Status',
                    'Value': 'ACTIVE',
                    'Condition': 'IS'
                }
            ]
        )
        predictors = response['Predictors']
        for predictor in predictors:
            predictor_set.add(predictor['PredictorName'])

        for forecast_name, forecast_arn in forecasts_arn_dict.items():
            response = forecast_client.list_forecast_export_jobs(
                Filters=[
                    {
                        'Key': 'ForecastArn',
                        'Value': forecast_arn,
                        'Condition': 'IS'
                    },
                    {
                        'Key': 'Status',
                        'Value': 'ACTIVE',
                        'Condition': 'IS'
                    }
                ]
            )
            forecast_exports = response['ForecastExportJobs']
            for forecast_export in forecast_exports:
                destination = forecast_export['Destination']
                s3_config = destination['S3Config']
                path = s3_config['Path']
                if s3_path in path:
                    forecast_exports_set.add(forecast_export['ForecastExportJobName'])
        intersection_set = forecast_exports_set.intersection(forecast_set)
        intersection_set = intersection_set.intersection(predictor_set)
        return(list(intersection_set)), 'success'
    except Exception as e:
        error(str(e))
        return None, 'failure'


def create_target_dataset(forecast_client, dataset_name, data_frequency, attributes):
    """Creates a new target dataset"""

    try:
        response = forecast_client.create_dataset(
            DatasetName=dataset_name,
            Domain='RETAIL',
            DatasetType='TARGET_TIME_SERIES',
            DataFrequency=data_frequency,
            Schema={
                'Attributes': attributes,
            }
        )
        return response['DatasetArn'], 'success'
    except Exception as e:
        error(str(e))
        return None, 'failure'


def create_related_dataset(forecast_client, dataset_name, data_frequency, attributes):
    try:
        """Creates a new related dataset"""

        response = forecast_client.create_dataset(
            DatasetName=dataset_name,
            Domain='RETAIL',
            DatasetType='RELATED_TIME_SERIES',
            DataFrequency=data_frequency,
            Schema={
                'Attributes': attributes,
            }
        )
        return response['DatasetArn'], 'success'
    except Exception as e:
        error(str(e))
        return None, 'failure'


def create_metadata_dataset(forecast_client, dataset_name, attributes):
    try:
        """Creates a new metadata dataset"""

        response = forecast_client.create_dataset(
            DatasetName=dataset_name,
            Domain='RETAIL',
            DatasetType='ITEM_METADATA',
            Schema={
                'Attributes': attributes,
            }
        )
        return response['DatasetArn'], 'success'
    except Exception as e:
        error(str(e))
        return None, 'failure'


def create_dataset_import_job(forecast_client, dataset_name, dataset_arn, s3_path, role_arn):
    """Creates a new dataset import job"""

    dataset_import_job_number = 1
    date = str(datetime.date(datetime.now()))
    date = date.replace('-', '')

    # Increment import job number if one already exists with this name
    while True:
        dataset_import_name = dataset_name + date + '_' + str(dataset_import_job_number)
        try:
            response = forecast_client.create_dataset_import_job(
                DatasetImportJobName=dataset_import_name,
                DatasetArn=dataset_arn,
                DataSource={
                    'S3Config': {
                        'Path': s3_path,
                        'RoleArn': role_arn,
                    }
                },
                TimestampFormat='yyyy-MM-dd HH:mm:ss'
            )
            return 'success'
        except forecast_client.exceptions.ResourceAlreadyExistsException:
            dataset_import_job_number += 1
        except Exception as e:
            error(str(e))
            return 'failure'


def link_dataset_to_dataset_group(forecast_client, dataset_arn_list, dataset_group_arn):
    """Connects a newly created dataset to a dataset group"""

    try:
        response = forecast_client.update_dataset_group(
            DatasetGroupArn=dataset_group_arn,
            DatasetArns=dataset_arn_list
        )
        return 'success'
    except Exception as e:
        error(str(e))
        return 'failure'


def create_predictor(forecast_client, predictor_name, algorithm, auto_ml, forecast_horizon, forecast_frequency,
                     dataset_group_arn, location_in_datasets=False, holidays=None):
    """Creates a new predictor"""

    predictor_attributes = {'PredictorName': predictor_name, 'ForecastHorizon': forecast_horizon}

    if auto_ml:
        predictor_attributes['PerformAutoML'] = True
    else:
        predictor_attributes['AlgorithmArn'] = algorithm

    if holidays is None:
        predictor_attributes['InputDataConfig'] = {
            'DatasetGroupArn': dataset_group_arn
        }
    else:
        predictor_attributes['InputDataConfig'] = {
            'DatasetGroupArn': dataset_group_arn,
            'SupplementaryFeatures': [{
                'Name': 'holiday',
                'Value': holidays
            }
            ]
        }

    if location_in_datasets:
        predictor_attributes['FeaturizationConfig'] = {
            'ForecastFrequency': forecast_frequency,
            'ForecastDimensions': [
                'location',
            ]
        }
    else:
        predictor_attributes['FeaturizationConfig'] = {
            'ForecastFrequency': forecast_frequency
        }

    try:
        response = forecast_client.create_predictor(**predictor_attributes)
        return response['PredictorArn'], 'success'
    except Exception as e:
        error(str(e))
        return None, 'failure'


def create_forecast(forecast_client, forecast_name, predictor_arn):
    """Creates a new forecast"""

    try:
        response = forecast_client.create_forecast(
            ForecastName=forecast_name,
            PredictorArn=predictor_arn
        )
        return response['ForecastArn'], 'success'
    except Exception as e:
        error(str(e))
        return None, 'failure'


def create_forecast_export(forecast_client, forecast_export_name, forecast_arn, s3_path, s3_role_arn):
    """Creates a new forecast export"""

    try:
        response = forecast_client.create_forecast_export_job(
            ForecastExportJobName=forecast_export_name,
            ForecastArn=forecast_arn,
            Destination={
                'S3Config': {
                    'Path': s3_path,
                    'RoleArn': s3_role_arn,
                }
            },
        )
        return response['ForecastExportJobArn'], 'success'
    except Exception as e:
        error(str(e))
        return None, 'failure'


def list_all_datasets_names(forecast_client):
    """Lists all existing datasets names"""

    try:
        response = forecast_client.list_datasets()
    except Exception as e:
        worker_error_page("Error!", str(e))
    datasets = response['Datasets']
    datasets_names = []
    for dataset in datasets:
        datasets_names.append(dataset['DatasetName'])
    return datasets_names


def get_import_jobs_status_list(forecast_client, dataset_arn):
    """get the list of import jobs statuses"""

    try:
        status_list = []
        response = forecast_client.list_dataset_import_jobs(
            Filters=[
                {
                    'Key': 'DatasetArn',
                    'Value': dataset_arn,
                    'Condition': 'IS'
                },
            ]
        )
        dataset_import_jobs = response['DatasetImportJobs']
        for dataset_import_job in dataset_import_jobs:
            status_list.append(dataset_import_job['Status'])
        return status_list

    except Exception as e:
        worker_error_page("Error!", str(e))


def list_all_forecasts(forecast_client):
    """Lists all existing forecasts"""

    try:
        response = forecast_client.list_predictors()
    except Exception as e:
        worker_error_page("Error!", str(e))
    predictors = response['Predictors']
    predictors_set = set()
    for predictor in predictors:
        predictors_set.add(predictor['PredictorName'])

    try:
        response = forecast_client.list_forecasts()
    except Exception as e:
        worker_error_page("Error!", str(e))
    forecasts = response['Forecasts']
    forecasts_set = set()
    for forecast in forecasts:
        forecasts_set.add(forecast['ForecastName'])
    return list(forecasts_set.union(predictors_set))


def create_dataset_group(forecast_client, dataset_group_name):
    """Creates a new dataset group"""

    try:
        response = forecast_client.create_dataset_group(DatasetGroupName=dataset_group_name, Domain="RETAIL")

    except Exception as e:
        error(str(e))
