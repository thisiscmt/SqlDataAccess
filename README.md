SQL Data Access
=============
This library is used as a data access layer for interacting with a Microsoft SQL Server database. It uses ADO.NET and has the following features:

* Execute ad-hoc parametrized SQL queries.
* Execute stored procedures, with or without return parameters for error reporting.
* Execute Insert statements and obtain the identity-based primary key (single field).
* Run SQL commands in a transaction.
* Convert a SqlDataReader to a DataSet or DataTable.

I've used it in both Windows client applications and ASP.NET web applications. It takes in a connection string and closes the server connection when an operation is completed. My goal was to make something very simple that I could reuse across projects, even if it were not as generic as it could be.
