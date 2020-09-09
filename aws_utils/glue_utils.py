import time

from gui.error_display_functions import error


def create_database(glue_client):
    try:
        response = glue_client.create_database(
            DatabaseInput={
                'Name': 'retail_demand_forecasting_db'
            }
        )
    except glue_client.exceptions.AlreadyExistsException:
        return 'success'
    except Exception as e:
        error(str(e))
        return 'failure'


def create_crawler(glue_client, role, s3_path, name):
    try:
        response = glue_client.create_crawler(
            Name=name,
            Role=role,
            DatabaseName='retail_demand_forecasting_db',
            Targets={
                'S3Targets': [
                    {
                        'Path': s3_path
                    }
                ]
            },
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DELETE_FROM_DATABASE'
            }
        )
        return 'success'
    except glue_client.exceptions.AlreadyExistsException:
        try:
            response = glue_client.update_crawler(
                Name=name,
                Role=role,
                DatabaseName='retail_demand_forecasting_db',
                Targets={
                    'S3Targets': [
                        {
                            'Path': s3_path,
                        }
                    ]
                },
                SchemaChangePolicy={
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'DELETE_FROM_DATABASE'
                }
            )
            return 'success'
        except Exception as e:
            error(str(e))
            return 'failure'
    except Exception as e:
        error(str(e))
        return 'failure'


def run_crawler(name, glue_client):
    try:
        response = glue_client.start_crawler(
            Name=name
        )
        time.sleep(5)
        response = glue_client.get_crawler(
            Name=name
        )
        crawler = response['Crawler']
        while crawler['State'] != 'READY':
            response = glue_client.get_crawler(
                Name=name
            )
            crawler = response['Crawler']
            time.sleep(5)
        return 'success'
    except Exception as e:
        error(str(e))
        return 'failure'
