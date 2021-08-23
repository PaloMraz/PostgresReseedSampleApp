using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using System.Transactions;

namespace PostgresReseedSampleApp
{
  class Program
  {
    static async Task Main(string[] args)
    {
      const string ConnectionString = "Server=127.0.0.1;Port=5433;Database=yb_demo;User Id=yugabyte;Password=;";

      // Create test table in default "public" scheme.
      using (var connection = new NpgsqlConnection(ConnectionString))
      {
        await connection.OpenAsync();
        await connection.ExecuteAsync(
          "drop table if exists __test; " + 
          "create table __test(id int4 not null generated always as identity, name varchar(50) not null);");
      }

      // Perform transacted inserts.
      try
      {
        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
          using (var connection = new NpgsqlConnection(ConnectionString))
          {
            await connection.OpenAsync();
            await connection.ExecuteAsync(
              "insert into __test (name) values ('n1'); " +
              "insert into __test (name) values ('n2'); " +
              "insert into __test (id, name) overriding system value values (100, 'n3'); " +
              "select setval('__test_id_seq', 1);");

            // This fails because name cannot be null, effectively rolling back the transaction.
            await connection.ExecuteAsync("insert into __test (name) values (null);");
          }

          scope.Complete();
        }
      }
      catch (Exception ex)
      {
        Console.WriteLine(ex.ToString());
      }

      using (var connection = new NpgsqlConnection(ConnectionString))
      {
        await connection.OpenAsync();

        // Verify there are no rows in the __test table.
        long rowCount = await connection.QuerySingleAsync<long>("select count(*) from __test;");
        Console.WriteLine($"RowCount = {rowCount}"); // <-- Displays correctly: "RowCount = 0".

        // Verify the sequence has been reseeded.
        long seed = await connection.QuerySingleAsync<long>("select nextval('__test_id_seq');");
        Console.WriteLine($"Seed = {seed}"); // <-- Displays : "Seed = 3", which is probably correct :-)
      }
    }
  }
}
