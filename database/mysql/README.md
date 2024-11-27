main.py contains
--- everything below, currently. 
--- will write basic stuff in main.py and gradually shift to other files, if necessary.

loader.py contains
--- functions to load the database

queries.py contains
--- functions to run basic analytical commands that might be useful for debugging/database usage, e.g. simple SELECTs

recommender.py contains
--- recommendation engine functions/queries

tests.py contains local tests for methods in queries.py

### Creating the user and database locally
Login to MySQL as admin. Run the following commands:
```
CREATE USER 'cs6400'@'localhost' IDENTIFIED BY 'qwertyuiop';
```
```
CREATE DATABASE cs6400_0;
```
```
GRANT ALL PRIVILEGES ON cs6400_0.* TO 'cs6400'@'localhost';
```
Following this, you should be able to connect to the database with something like
```python
connection = mysql.connector.connect(
            host="localhost",
            user="cs6400",
            password="qwertyuiop",
            database="cs6400_0"
        )
```