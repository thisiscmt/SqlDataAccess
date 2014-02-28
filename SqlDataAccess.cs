using System;
using System.Data;
using System.Data.SqlClient;
using System.Collections;
using System.Collections.Generic;

namespace SqlDataAccess
{
    /// <summary>
    /// Provides an interface to execute SQL commands and stored procedures against a Microsoft SQL Server database.
    /// </summary>
    public class SqlDataAccess : IDisposable
    {
        private List<SqlParameter> m_parms;
        private SqlConnection m_conn;
        private SqlTransaction m_transaction;
        private string m_connStr;
        private string m_transName;
        private bool m_transInProgress;
        private bool m_disposed = false;

        #region Constructor
            /// <param name="connStr">Connection string for the target database.</param>
            public SqlDataAccess(string connStr)
            {
                m_transInProgress = false;
                m_connStr = connStr;
                m_parms = new List<SqlParameter>();
            }
        #endregion

        #region Methods
        /// <summary>
        /// Executes a Select statement and returns the result set.
        /// </summary>
        /// <param name="sql">SQL statement to be executed.</param>
        /// <param name="timeout">Amount of time to allow the command to run.</param>
        /// <returns>Data table containing the set of records returned by the SQL statement.</returns>
        public DataTable ExecuteDataTable(string sql, int timeout = -1)
        {
            SqlDataAdapter da = null;
            SqlCommand cmd = null;
            DataTable dt = null;

            try
            {
                cmd = CreateSqlCommand(sql, timeout);
                da = new SqlDataAdapter(cmd);
                dt = new DataTable();
                da.Fill(dt);

                return dt;
            }
            catch
            {
                if (m_transInProgress)
                {
                    RollbackTransaction();
                }

                throw;
            }
            finally
            {
                if (cmd != null)
                {
                    if (!m_transInProgress)
                    {
                        cmd.Connection.Close();
                    }

                    cmd.Dispose();
                }

                if (da != null)
                {
                    da.Dispose();
                }

                m_parms.Clear();
            }
        }

        /// <summary>
        /// Executes a Select statement and returns the results in a data set.
        /// </summary>
        /// <param name="sql">SQL statement to be executed.</param>
        /// <param name="timeout">Amount of time to allow the command to run.</param>
        public DataSet ExecuteDataSet(string sql, int timeout = -1)
        {
            SqlDataAdapter da = null;
            SqlCommand cmd = null;
            DataSet ds = null;

            try
            {
                cmd = CreateSqlCommand(sql, timeout);
                da = new SqlDataAdapter(cmd);
                ds = new DataSet();
                da.Fill(ds);

                return ds;
            }
            catch
            {
                throw;
            }
            finally
            {
                if (cmd != null)
                {
                    if (!m_transInProgress)
                    {
                        cmd.Connection.Close();
                    }

                    cmd.Dispose();
                }

                if (da != null)
                {
                    da.Dispose();
                }

                m_parms.Clear();
            }
        }

        /// <summary>
        /// Executes a non-Select SQL statement.
        /// </summary>
        /// <param name="sql">SQL statement to be executed.</param>
        /// <param name="timeout">Amount of time to allow the command to run.</param>
        /// <returns>Number of rows affected.</returns>
        public int ExecuteNonQuery(string sql, int timeout = -1)
        {
            SqlCommand cmd = null;

            try
            {
                cmd = CreateSqlCommand(sql, timeout);
                return cmd.ExecuteNonQuery();
            }
            catch
            {
                if (m_transInProgress)
                {
                    RollbackTransaction();
                }

                throw;
            }
            finally
            {
                if (cmd != null)
                {
                    if (!m_transInProgress)
                    {
                        cmd.Connection.Close();
                    }

                    cmd.Dispose();
                }

                m_parms.Clear();
            }
        }

        /// <summary>
        /// Executes an Insert SQL statement.
        /// </summary>
        /// <param name="sql">SQL statement to be executed.</param>
        /// <param name="id">ID value of the inserted record.</param>
        /// <param name="timeout">Amount of time to allow the command to run.</param>
        /// <returns>Number of rows affected.</returns>
        public int ExecuteInsertGetID(string sql, ref int id, int timeout = -1)
        {
            SqlCommand cmd = null;
            int retVal = 0;

            try
            {
                cmd = CreateSqlCommand(sql, timeout);
                retVal = cmd.ExecuteNonQuery();
                cmd.Parameters.Clear();
                cmd.CommandText = "Select @@IDENTITY As [ID]";
                id = Convert.ToInt32(cmd.ExecuteScalar());

                return retVal;
            }
            catch
            {
                if (m_transInProgress)
                {
                    RollbackTransaction();
                }

                throw;
            }
            finally
            {
                if (cmd != null)
                {
                    if (!m_transInProgress)
                    {
                        cmd.Connection.Close();
                    }

                    cmd.Dispose();
                }

                m_parms.Clear();
            }
        }

        /// <summary>
        /// Executes a scalar SQL statement.
        /// </summary>
        /// <param name="sql">SQL statement to be executed.</param>
        /// <param name="timeout">Amount of time to allow the command to run.</param>
        /// <returns>The first field of the first row of the result set.</returns>
        public object ExecuteScalar(string sql, int timeout = -1)
        {
            SqlCommand cmd = null;

            try
            {
                cmd = CreateSqlCommand(sql, timeout);
                return cmd.ExecuteScalar();
            }
            catch
            {
                if (m_transInProgress)
                {
                    RollbackTransaction();
                }

                throw;
            }
            finally
            {
                if (cmd != null)
                {
                    if (!m_transInProgress)
                    {
                        cmd.Connection.Close();
                    }

                    cmd.Dispose();
                }

                m_parms.Clear();
            }
        }

        /// <summary>
        /// Executes a Select stored procedure.
        /// </summary>
        /// <param name="procName">Name of the procedure to execute.</param>
        /// <param name="timeout">Amount of time to allow the command to run.</param>
        /// <param name="parms">Arguments for the procedure.</param>
        public DataTable ExecuteSelectSP(string procName, int timeout, params object[] parms)
        {
            SqlDataAdapter da = null;
            SqlCommand cmd = null;
            DataTable dt = null;

            try
            {
                cmd = CreateSPCommand(procName, timeout, parms);
                da = new SqlDataAdapter(cmd);
                dt = new DataTable();
                da.Fill(dt);

                return dt;
            }
            catch
            {
                if (m_transInProgress)
                {
                    RollbackTransaction();
                }

                throw;
            }
            finally
            {
                if (cmd != null)
                {
                    if (!m_transInProgress)
                    {
                        cmd.Connection.Close();
                    }

                    cmd.Dispose();
                }

                if (da != null)
                {
                    da.Dispose();
                }
            }
        }

        /// <summary>
        /// Executes a Select stored procedure that includes error notifications.
        /// </summary>
        /// <param name="procName">Name of the procedure to execute.</param>
        /// <param name="timeout">Amount of time to allow the command to run.</param>
        /// <param name="retCode">Error code returned by the procedure.</param>
        /// <param name="retMsg">Error message returned by the procedure.</param>
        /// <param name="parms">Arguments for the procedure.</param>
        public DataTable ExecuteSelectSPErrorParms(string procName, int timeout, ref int retCode, ref string retMsg, params object[] parms)
        {
            SqlCommand cmd = null;
            SqlDataAdapter da = null;
            DataTable dt = null;

            try
            {
                cmd = CreateSPCommand(procName, timeout, parms);
                da = new SqlDataAdapter(cmd);
                dt = new DataTable();
                da.Fill(dt);

                retCode = Convert.ToInt32(cmd.Parameters["@Ret_Code"].Value);
                retMsg = cmd.Parameters["@Ret_Message"].Value.ToString();
                return dt;
            }
            catch
            {
                if (m_transInProgress)
                {
                    RollbackTransaction();
                }

                throw;
            }
            finally
            {
                if (cmd != null)
                {
                    if (!m_transInProgress)
                    {
                        cmd.Connection.Close();
                    }

                    cmd.Dispose();
                }

                if (da != null)
                {
                    da.Dispose();
                }
            }
        }

        /// <summary>
        /// Executes a non-Select stored procedure.
        /// </summary>
        /// <param name="procName">Name of the procedure to execute.</param>
        /// <param name="timeout">Amount of time to allow the command to run.</param>
        /// <returns>Number of rows affected.</returns>
        public int ExecuteNonQuerySP(string procName, int timeout, params object[] parms)
        {
            SqlCommand cmd = null;

            try
            {
                cmd = CreateSPCommand(procName, timeout, parms);
                return cmd.ExecuteNonQuery();
            }
            catch
            {
                if (m_transInProgress)
                {
                    RollbackTransaction();
                }

                throw;
            }
            finally
            {
                if (cmd != null)
                {
                    if (!m_transInProgress)
                    {
                        cmd.Connection.Close();
                    }

                    cmd.Dispose();
                }
            }
        }

        /// <summary>
        /// Executes a non-Select stored procedure.
        /// </summary>
        /// <param name="procName">Name of the procedure to execute.</param>
        /// <param name="timeout">Amount of time to allow the command to run.</param>
        /// <param name="outputParm">Value to be output from the procedure.</param>
        /// <returns>Number of rows affected.</returns>
        public object ExecuteNonQuerySPOutput(string procName, int timeout, string outputParm, params object[] parms)
        {
            SqlCommand cmd = null;

            try
            {
                cmd = CreateSPCommand(procName, timeout, parms);
                cmd.ExecuteNonQuery();

                return cmd.Parameters[outputParm].Value;
            }
            catch
            {
                if (m_transInProgress)
                {
                    RollbackTransaction();
                }

                throw;
            }
            finally
            {
                if (cmd != null)
                {
                    if (!m_transInProgress)
                    {
                        cmd.Connection.Close();
                    }

                    cmd.Dispose();
                }
            }
        }

        /// <summary>
        /// Executes a non-Select stored procedure that includes error notifications.
        /// </summary>
        /// <param name="procName">Name of the procedure to execute.</param>
        /// <param name="timeout">Amount of time to allow the command to run.</param>
        /// <param name="retCode">Error code returned by the procedure.</param>
        /// <param name="retMsg">Error message returned by the procedure.</param>
        /// <param name="parms">Arguments for the procedure.</param>
        public int ExecuteNonQuerySPErrorParms(string procName, int timeout, ref int retCode, ref string retMsg, params object[] parms)
        {
            SqlCommand cmd = null;
            int retVal = 0;

            try
            {
                cmd = CreateSPCommand(procName, timeout, parms);
                retVal = cmd.ExecuteNonQuery();

                retCode = Convert.ToInt32(cmd.Parameters["@Ret_Code"].Value);
                retMsg = cmd.Parameters["@Ret_Message"].Value.ToString();
                return retVal;
            }
            catch
            {
                if (m_transInProgress)
                {
                    RollbackTransaction();
                }

                throw;
            }
            finally
            {
                if (cmd != null)
                {
                    if (!m_transInProgress)
                    {
                        cmd.Connection.Close();
                    }

                    cmd.Dispose();
                }
            }
        }

        /// <summary>
        /// Executes a non-Select stored procedure that includes error notifications.
        /// </summary>
        /// <param name="procName">Name of the procedure to execute.</param>
        /// <param name="timeout">Amount of time to allow the command to run.</param>
        /// <param name="retCode">Error code returned by the procedure.</param>
        /// <param name="retMsg">Error message returned by the procedure.</param>
        /// <param name="outputParm">Value to be output from the procedure.</param>
        /// <param name="parms">Arguments for the procedure.</param>
        public object ExecuteNonQuerySPOutputErrorParms(string procName, int timeout, ref int retCode, ref string retMsg, string outputParm, params object[] parms)
        {
            SqlCommand cmd = null;

            try
            {
                cmd = CreateSPCommand(procName, timeout, parms);
                cmd.ExecuteNonQuery();

                retCode = Convert.ToInt32(cmd.Parameters["@Ret_Code"].Value);
                retMsg = cmd.Parameters["@Ret_Message"].Value.ToString();
                return cmd.Parameters[outputParm].Value;
            }
            catch
            {
                if (m_transInProgress)
                {
                    RollbackTransaction();
                }

                throw;
            }
            finally
            {
                if (cmd != null)
                {
                    if (!m_transInProgress)
                    {
                        cmd.Connection.Close();
                    }

                    cmd.Dispose();
                }
            }
        }

        /// <summary>
        /// Adds a parameter for later execution of a paramaterized SQL statement.
        /// </summary>
        /// <param name="parmName">Name of the parameter.</param>
        /// <param name="value">Value of the parameter</param>
        /// <param name="direction">Direction of the paramater.</param>
        public void AddSQLParmeter(string parmName, object value, ParameterDirection direction = ParameterDirection.Input)
        {
            SqlParameter parm = null;

            parm = new SqlParameter();
            parm.IsNullable = true;
            parm.ParameterName = parmName;
            parm.Value = value;
            parm.Direction = direction;

            m_parms.Add(parm);
        }

        /// <summary>
        /// Initiates a transaction with the current server and database.
        /// </summary>
        /// <param name="transName">Name of the transaction.</param>
        public void BeginTransaction(string transName)
        {
            try
            {
                m_conn = new SqlConnection(m_connStr);
                m_conn.Open();
                m_transName = transName;
                m_transaction = m_conn.BeginTransaction(transName);
                m_transInProgress = true;
            }
            catch
            {
                if (m_transInProgress)
                {
                    RollbackTransaction();
                }
                else
                {
                    CloseConnection();
                }

                throw;
            }
        }

        /// <summary>
        /// Initiates a transaction with the current server and database.
        /// </summary>
        /// <param name="isolationLevel">Isolation level of the transaction.</param>
        /// <param name="transName">Name of the transaction.</param>
        public void BeginTransaction(IsolationLevel isolationLevel, string transName)
        {
            try
            {
                m_conn = new SqlConnection(m_connStr);
                m_conn.Open();
                m_transName = transName;
                m_transaction = m_conn.BeginTransaction(isolationLevel, transName);
                m_transInProgress = true;
            }
            catch
            {
                if (m_transInProgress)
                {
                    RollbackTransaction();
                }
                else
                {
                    CloseConnection();
                }

                throw;
            }
        }

        /// <summary>
        /// Commits an open transaction.
        /// </summary>
        public void CommitTransaction()
        {
            try
            {
                if (m_transInProgress)
                {
                    m_transaction.Commit();
                }
            }
            catch
            {
                if (m_transInProgress)
                {
                    RollbackTransaction();
                }

                throw;
            }
            finally
            {
                if (m_transInProgress)
                {
                    m_transaction.Dispose();
                    m_transInProgress = false;
                    m_transName = "";
                    m_parms.Clear();

                    CloseConnection();
                }
            }
        }

        /// <summary>
        /// Rolls back an open transaction.
        /// </summary>
        public void RollbackTransaction()
        {
            try
            {
                if (m_transInProgress)
                {
                    m_transaction.Rollback(m_transName);
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if (m_transInProgress)
                {
                    m_transaction.Dispose();
                    m_transInProgress = false;
                    m_transName = "";
                    m_parms.Clear();

                    CloseConnection();
                }
            }
        }

        /// <summary>
        /// Closes the connection to the current SQL server.
        /// </summary>
        public void CloseConnection()
        {
            if (m_conn != null)
            {
                m_conn.Dispose();
            }
        }
        #endregion

        #region Shared methods
        /// <summary>
        /// Converts a SqlDataReader to a dataset.
        /// </summary>
        /// <param name="reader">Reader to be converted</param>
        /// <returns>Dataset containing all records from the reader.</returns>
        public static DataSet ConvertDataReaderToDataSet(SqlDataReader reader)
        {
            DataSet ds = null;

            ds = new DataSet();
            ds.Tables.Add(ConvertDataReaderToDataTable(reader));

            return ds;
        }

        /// <summary>
        /// Converts a SqlDataReader to a data table.
        /// </summary>
        /// <param name="reader">Reader to be converted</param>
        /// <returns>Data table containing all records from the reader.</returns>
        public static DataTable ConvertDataReaderToDataTable(SqlDataReader reader)
        {
            DataTable dt = null;
            int fieldCount = 0;
            int i = 0;

            dt = new DataTable();
            fieldCount = reader.FieldCount;

            for (i = 0; i <= fieldCount - 1; i++)
            {
                dt.Columns.Add(reader.GetName(i), reader.GetFieldType(i));
            }

            dt.BeginLoadData();

            object[] aValues = new object[fieldCount];
            while (reader.Read())
            {
                reader.GetValues(aValues);
                dt.LoadDataRow(aValues, true);
            }

            reader.Close();
            dt.EndLoadData();

            return dt;
        }
        #endregion

        #region Private routines
        private SqlCommand CreateSqlCommand(string sql, int timeout = -1)
        {
            // Input:        sql - command text to be run, either a SQL statement or table name
            //               timeout - amount of time to allow the command to run
            // Return Value: Command object for executing the statement.
            // Description:  Inserts the contents of m_parms into a SQL command object,
            //               then returns that object for later execution.

            SqlCommand cmd = null;

            try
            {
                cmd = new SqlCommand();
                if (timeout != -1)
                {
                    cmd.CommandTimeout = timeout;
                }

                // If a transaction is ocurring, use the existing connection rather
                // than creating a new one
                if (m_transInProgress)
                {
                    cmd.Connection = m_conn;
                    cmd.Transaction = m_transaction;
                }
                else
                {
                    cmd.Connection = new SqlConnection(m_connStr);

                    // We open the connection regardless of the type of statement being 
                    // executed, because even though ExecuteDataTable() will open and 
                    // close it automatically, the methods for inserting and updating 
                    // will require an open connection to the database
                    cmd.Connection.Open();
                }

                cmd.CommandType = CommandType.Text;
                cmd.CommandText = sql;

                foreach (SqlParameter parm in m_parms)
                {
                    if (parm.Value == null)
                    {
                        cmd.Parameters.AddWithValue(parm.ParameterName, DBNull.Value);
                    }
                    else
                    {
                        cmd.Parameters.Add(parm);
                    }
                }

                return cmd;
            }
            catch
            {
                if (m_transInProgress)
                {
                    RollbackTransaction();
                }

                throw;
            }
        }

        private SqlCommand CreateSPCommand(string procName, int timeout, params object[] parms)
        {
            // Input:        procName - name of the stored procedure to execute
            //               timeout - amount of time to allow the command to run, -1 to use the default
            // Return Value: Command object for executing the procedure.
            // Description:  Inserts the contents of m_parms into a SQL command object,
            //               then returns that object for later execution.


            SqlCommand cmd = null;
            int parmIndex = 0;
            int argIndex = 0;
            int i = 0;

            try
            {
                cmd = new SqlCommand();
                if (timeout != -1)
                {
                    cmd.CommandTimeout = timeout;
                }

                // If a transaction is ocurring, use the existing connection rather
                // than create a new one
                if (m_transInProgress)
                {
                    cmd.Connection = m_conn;
                    cmd.Transaction = m_transaction;
                }
                else
                {
                    cmd.Connection = new SqlConnection(m_connStr);
                    cmd.Connection.Open();
                }

                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = procName;
                SqlCommandBuilder.DeriveParameters(cmd);

                // Assign all parameter values passed in from the caller to the 
                // command object. We start with index 1 since that's where the
                // actual parms begin in the Parameters collection
                argIndex = 0;
                parmIndex = 1;
                for (i = 0; i <= parms.Length - 1; i++)
                {
                    if (parms[argIndex] == null)
                    {
                        cmd.Parameters[parmIndex].Value = DBNull.Value;
                    }
                    else
                    {
                        cmd.Parameters[parmIndex].Value = parms[argIndex];
                    }

                    parmIndex = parmIndex + 1;
                    argIndex = argIndex + 1;
                }

                // If the caller didn't provide values for all the parameters, set
                // the remaining ones to null
                if (argIndex < cmd.Parameters.Count - 1)
                {
                    for (i = argIndex + 1; i <= cmd.Parameters.Count - 1; i++)
                    {
                        cmd.Parameters[i].Value = DBNull.Value;
                    }
                }

                return cmd;
            }
            catch
            {
                if (m_transInProgress)
                {
                    RollbackTransaction();
                }

                throw;
            }
        }
        #endregion

        #region Dispose routines
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!m_disposed)
            {
                if (disposing)
                {
                    CloseConnection();
                    m_parms.Clear();
                }

                // Dispose of unmanaged resources
            }

            m_disposed = true;
        }
        #endregion
    }
}


