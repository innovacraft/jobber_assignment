import configparser
import os

def get_redshift_connection_properties():
    """
    Parses the dbconnect.conf file to retrieve Redshift connection properties.
    """
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), '../config/dbconnect.conf')
    config.read(config_path)

    redshift_url = config.get('Redshift', 'url')
    properties = {
        "driver": config.get('Redshift', 'driver')
    }
    return redshift_url, properties
