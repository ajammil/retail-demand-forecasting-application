import time

from gui.error_display_functions import error


def is_location_in_table(athena_client, table_name):
    try:
        response = athena_client.get_table_metadata(
            CatalogName='AwsDataCatalog',
            DatabaseName='retail_demand_forecasting_db',
            TableName=table_name
        )
    except Exception as e:
        error(str(e))
        return None, 'failure'
    table_meta_data = response['TableMetadata']
    columns = table_meta_data['Columns']
    location_in_table = False
    for column in columns:
        if column['Name'] == 'location':
            location_in_table = True
    return location_in_table, 'success'


def run_forecast_query(athena_client, table_name, s3_path, item_id, location=None):
    try:
        print(s3_path + '/queries/')
        if location is None:
            response = athena_client.start_query_execution(
                QueryString='SELECT * FROM ' + table_name + ' WHERE cast(item_id as varchar) = \'' + item_id + '\'',
                ResultConfiguration={
                    'OutputLocation': s3_path + '/queries/'},
                QueryExecutionContext={
                    'Database': 'retail_demand_forecasting_db',
                    'Catalog': 'AwsDataCatalog'
                },
            )
        else:
            response = athena_client.start_query_execution(
                QueryString='SELECT * FROM ' + table_name + ' WHERE cast(item_id as varchar) = \'' + item_id +
                            '\' AND cast(location as varchar) = \'' + location + '\'',
                ResultConfiguration={
                    'OutputLocation': s3_path + '/queries/'},
                QueryExecutionContext={
                    'Database': 'retail_demand_forecasting_db',
                    'Catalog': 'AwsDataCatalog'
                },
            )
        execution_id = response["QueryExecutionId"]
        response = athena_client.get_query_execution(
            QueryExecutionId=execution_id
        )
        query_execution = response['QueryExecution']
        query_status = query_execution['Status']
        state = query_status['State']
        while state != 'SUCCEEDED' and state != 'FAILED' and state != 'CANCELLED':
            time.sleep(0.5)
            response = athena_client.get_query_execution(
                QueryExecutionId=execution_id
            )
            query_execution = response['QueryExecution']
            query_status = query_execution['Status']
            state = query_status['State']
        if state == 'SUCCEEDED':
            data_file_name = f'{execution_id}.csv'
            return data_file_name, 'success'
        else:
            raise Exception("Query Failed or Cancelled")

    except Exception as e:
        error(str(e))
        return None, 'failure'
