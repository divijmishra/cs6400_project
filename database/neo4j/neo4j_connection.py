from neo4j import GraphDatabase
import numpy as np
from datetime import datetime, timedelta
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Neo4jConnection:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        self.driver.close()
        
    def query(self, query, parameters=None, session=None):
        """
        Flexible query method with optional existing session
        """
        if session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]
        
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]