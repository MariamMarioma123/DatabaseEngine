# DatabaseEngine
The project aims to build a small database engine with support for Octrees Indices. The engine should have the following functionalities:

-Creating tables
-Inserting tuples into tables
-Deleting tuples from tables
-Searching in tables linearly
-Creating an Octree upon demand
-Using the created Octree(s) where appropriate

It is important to note that:
-The engine should store each table as separate pages on disk, with each page having a fixed maximum number of rows(this data is known from the config file). 
-Pages should be stored as serialized Vectors. Each tuple should be stored in a separate object within the binary file.
-A metadata file should be used to store information about each user table, including the number of columns, data types of columns, and indices built on them. The metadata should be stored in a CSV file.
-Indices should be implemented using Octrees(index created on exactly 3 columns), and each column should have an associated range for creating divisions on the index scale.
-The engine should provide methods for initializing, creating tables, creating indices, inserting rows, updating rows, and deleting rows. It should also support selecting rows based on SQL terms and operators.
-The main class of the engine should be called DBApp.java, and it should include the required methods with their specified signatures. The parameters for the methods are passed as strings but should be converted to the correct types internally.
-Note that foreign keys and referential integrity constraints are not required to be supported in this simplified database engine. 
