using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppLockDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync();
            Console.ReadLine();
        }

        private static async void MainAsync()
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder["Data Source"] = ".";
            builder["Integrated Security"] = true;
            builder["Initial Catalog"] = "MyLab";

            try
            {
                var connExclusiveLocked = new SqlConnection(builder.ConnectionString);
                var tranExclusiveLocked = Lock(connExclusiveLocked);
                await Task.Delay(TimeSpan.FromSeconds(new Random().Next(6, 12)));

                Unlock(connExclusiveLocked, tranExclusiveLocked);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception occurs {ex.Message}");
            }
        }

        private static SqlTransaction Lock(SqlConnection conn)
        {
            Console.WriteLine("Begin sp_GetAppLock");
            SqlTransaction tran = null;
            try
            {
                conn.Open();
                tran = conn.BeginTransaction(System.Data.IsolationLevel.Serializable);

                using (var cmd = new SqlCommand("sp_GetAppLock", conn, tran))
                {
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddWithValue("Resource", "BlobCheckExisting");
                    cmd.Parameters.AddWithValue("LockMode", "Exclusive");
                    cmd.Parameters.AddWithValue("LockTimeout", (Int32)TimeSpan.FromSeconds(5).TotalMilliseconds);
                    SqlParameter returnValue = cmd.Parameters.Add("ReturnValue", SqlDbType.Int);
                    returnValue.Direction = ParameterDirection.ReturnValue;

                    cmd.ExecuteNonQuery();
                    var returnCode = (int)returnValue.Value;

                    if (returnCode < 0)
                    {
                        string errorMsg = "";
                        switch (returnCode)
                        {
                            case -1:
                                errorMsg = "The lock request timed out.";
                                break;
                            case -2:
                                errorMsg = "The lock request was canceled.";
                                break;
                            case -3:
                                errorMsg = "The lock request was chosen as a deadlock victim.";
                                break;
                            case -999:
                                errorMsg = "Parameter validation or other call error.";
                                break;
                        }

                        throw new Exception($"sp_getapplock failed with error code {returnCode}. {errorMsg}.");
                    }

                    Console.WriteLine("sp_GetAppLock succeed");
                }
            }
            catch (Exception)
            {
                if (tran != null)
                {
                    tran.Rollback();
                    tran.Dispose();
                }
                conn.Close();
                conn.Dispose();

                throw;
            }

            return tran;
        }

        private static void Unlock(SqlConnection conn, SqlTransaction tran)
        {
            try
            {
                Console.WriteLine("Begin to commit(/unlcok) sp_GetAppLock");
                tran.Commit();
                Console.WriteLine("Finished to commit(/unlcok) sp_GetAppLock");
            }
            catch (Exception)
            {
                tran.Rollback();

                throw;
            }
            finally
            {
                tran.Dispose();
                conn.Close();
                conn.Dispose();
            }
        }
    }
}
