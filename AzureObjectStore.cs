using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;
using System.IO;
using System.Runtime.Serialization;

using Microsoft.WindowsAzure.StorageClient;

using JoshCodes.Persistence.Azure.Sql.Extensions;
using System.Net;

namespace JoshCodes.Persistence.Azure.Sql
{
    public class AzureObjectStore<TDefine, TWrapper, TEntity>
        where TEntity : TableServiceEntity
        where TWrapper : AzureObjectWrapper<TEntity>, TDefine
    {
        protected delegate TWrapper CreateObjectStore(TEntity entity);

        protected CloudTableClient _tableClient;
        protected string _entityTableName;
        protected CreateObjectStore _createObjectStore;

        protected AzureObjectStore(CloudTableClient tableClient, string entityTableName, CreateObjectStore createObjectStore)
        {
            _tableClient = tableClient;
            _entityTableName = entityTableName;
            _createObjectStore = createObjectStore;
        }

        protected TDefine Find(string partitionKey, string rowKey)
        {
            var results = from entity in Query
                          where entity.RowKey == rowKey && entity.PartitionKey == partitionKey
                          select entity;
            try
            {
                foreach (var result in results)
                {
                    return _createObjectStore(result);
                }
            }
            catch (System.Data.Services.Client.DataServiceQueryException)
            {

            }
            return default(TDefine);
        }

        protected System.Data.Services.Client.DataServiceQuery<TEntity> Query
        {
            get
            {
                var tableServiceContext = _tableClient.GetDataServiceContext();
                tableServiceContext.IgnoreResourceNotFoundException = true;
                var query = tableServiceContext.CreateQuery<TEntity>(_entityTableName);
                return query;
            }
        }

        public TDefine FindByUrn(Uri urn)
        {
            string partitionKey;
            var rowKey = urn.ParseRowKey(_tableClient, out partitionKey);

            return Find(partitionKey, rowKey);
        }

        public IEnumerable<TDefine> All()
        {
            _tableClient.CreateTableIfNotExist(_entityTableName);
            var tableServiceContext = _tableClient.GetDataServiceContext();

            var query = tableServiceContext.CreateQuery<TEntity>(_entityTableName);
            var executedQuery = query.Execute();
            var results = from entity in executedQuery
                          select _createObjectStore.Invoke(entity);
            return results;
        }

        private class AutoIncrementStorage : TableServiceEntity
        {
            public long Value { get; set; }
        }

        protected static long AutoIncrementedValue(CloudTableClient tableClient, string entityTableName, string partitionKey, string rowKey)
        {
            var tableServiceContext = tableClient.GetDataServiceContext();
            do
            {
                tableServiceContext.IgnoreResourceNotFoundException = true;
                var query = tableServiceContext.CreateQuery<AutoIncrementStorage>(entityTableName);
                var results = from entity in query
                              where entity.RowKey == rowKey && entity.PartitionKey == partitionKey
                              select entity;
                var resultsList = results.ToList();
                try
                {
                    long autoIncrementValue;
                    if (resultsList.Count() == 0)
                    {
                        tableClient.CreateTableIfNotExist(entityTableName);
                        var storage = new AutoIncrementStorage()
                        {
                            Value = 2,
                            RowKey = rowKey,
                            PartitionKey = partitionKey
                        };
                        autoIncrementValue = 1;
                        tableServiceContext.AddObject(entityTableName, storage);
                    }
                    else
                    {
                        var storage = results.First();
                        autoIncrementValue = storage.Value;
                        storage.Value++;
                        tableServiceContext.UpdateObject(storage);
                    }

                    tableServiceContext.SaveChanges();
                    return autoIncrementValue;
                }
                catch (Exception ex)
                {
                    if (ex.IsProblemResourceAlreadyExists())
                    {
                        continue;
                    }
                    throw;
                }
            } while (true);
        }
    }
}
