import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool

class MySQLConnection:
    """
    Object to store a Connection to a MySQL database
    """
    def __init__(self, host, user, password, num_businesses):
        """
        num_businesses = 100, 1000, etc.
        """
        self.num_businesses = num_businesses
        try:
            self.connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=f"cs6400_{num_businesses}"
            )
            self.cursor = self.connection.cursor()
            
        except mysql.connector.Error as e:
            print(f"Error: {e}")

    