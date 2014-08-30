using System;
using JoshCodes.Persistence.Azure.Storage.Extensions;
using Microsoft.WindowsAzure.Storage.Table;


namespace JoshCodes.Persistence.Azure.Storage
{
    internal class UniquenessEntity : TableEntity//TableServiceEntity
    {

    }

    public static class CloudTableClientExtensions
    {
        public static bool TryRegisterUnique(this CloudTableClient tableClient, string uniqueId, string ns)
        {
            var entityTableName = typeof(UniquenessEntity).Name;
            var table = tableClient.GetTableReference(entityTableName);
            table.CreateIfNotExists();



            var entity = new UniquenessEntity()
            {
                RowKey = uniqueId,
                PartitionKey = ns,
            };

            try
            {
                TableOperation insertOperation = TableOperation.Insert(entity);
                table.Execute(insertOperation);
                return true;
            }
            catch (Exception ex)
            {
                if (ex.IsProblemResourceAlreadyExists())
                {
                    return false;
                }
                throw;
            }
        }
    }
}
