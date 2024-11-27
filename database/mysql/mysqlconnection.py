import mysql.connector

class MySQLConnection:
    """
    Object to store a Connection to a MySQL database
    """
    def __init__(self, host, user, password, num_businesses):
        """
        num_businesses = 100, 1000, etc.
        """
        try:
            self.num_businesses = num_businesses
            self.connection = mysql.connector.connect(
                host="localhost",
                user="cs6400",
                password="qwertyuiop",
                database=f"cs6400_{num_businesses}"
            )
            self.cursor = self.connection.cursor()
            
        except mysql.connector.Error as e:
            print(f"Error: {e}")

    def close(self):
        self.cursor.close()
        self.connection.close()
    