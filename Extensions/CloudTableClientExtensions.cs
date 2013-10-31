using System;

using Microsoft.WindowsAzure.StorageClient;

using JoshCodes.Persistence.Azure.Storage.Extensions;

namespace JoshCodes.Persistence.Azure.Storage
{
    internal class UniquenessEntity : TableServiceEntity
    {

    }

    public static class CloudTableClientExtensions
    {
        public static bool TryRegisterUnique(this CloudTableClient tableClient, string uniqueId, string ns)
        {
            var entityTableName = typeof(UniquenessEntity).Name;
            tableClient.CreateTableIfNotExist(entityTableName);

            var entity = new UniquenessEntity()
            {
                RowKey = uniqueId,
                PartitionKey = ns,
            };

            try
            {
                var tableServiceContext = tableClient.GetDataServiceContext();
                tableServiceContext.AddObject(entityTableName, entity);
                tableServiceContext.SaveChanges();
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
