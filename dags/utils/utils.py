import yaml
def conn_to_(dbname:str):
    """
        Подбираем параметры для подключения database
    """
    with open('/opt/airflow/config/db_config.yaml', 'r') as file:
        info=yaml.safe_load(file)
    if isinstance(dbname,str) and dbname=='postgres':
        print('Postgres is selected')
        return info['postgres']
    elif isinstance(dbname,str) and dbname=='s3':
        print('S3 is selected')
        return info['s3']
    else:
        print('No such database')
        return None