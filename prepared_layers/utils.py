import logging
import os
import json

# Configuration from environment variables
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
POSTGRES_DB_ANALYTICS = os.getenv('POSTGRES_DB_ANALYTICS', 'wtc_analytics')
POSTGRES_DB_PROD = os.getenv('POSTGRES_DB_PROD', 'wtc_prod')

# Constants for WAK (pricing)
CALL_RATE_ZAR_PER_MINUTE = 1.0  
DATA_RATE_ZAR_PER_GB = 49.0     


class JsonFormatter(logging.Formatter):
    """Formatter to output JSON log records."""
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage()
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)

def setup_logging():
    """Setup structured JSON logging."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter(datefmt='%Y-%m-%d %H:%M:%S'))
    
    # Remove existing handlers to avoid duplicates
    logger.handlers = []
    logger.addHandler(handler)


def get_logger(name):
    """Get a logger instance."""
    return logging.getLogger(name)

def send_alert(alert_name, severity, message):
    """
    Functional stub for critical alerting.
    In a real production system, this would integration with PagerDuty, Slack, or Email.
    """
    logger = get_logger('AlertManager')
    alert_msg = f"ALERT [{alert_name}] Severity: {severity} - {message}"
    
    # Log as warning/error depending on severity
    if severity.upper() == 'CRITICAL':
        logger.error(alert_msg)
    else:
        logger.warning(alert_msg)
        
    return True
