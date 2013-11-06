using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;
using System.IO;
using System.Runtime.Serialization;
using System.Net;
using System.Linq.Expressions;

using Microsoft.WindowsAzure.StorageClient;

using JoshCodes.Persistence.Azure.Storage.Extensions;
using JoshCodes.Web.Models.Domain;

namespace JoshCodes.Persistence.Azure.Storage
{
    public abstract class AzureObjectStore<TDefine, TWrapper, TEntity>
        where TEntity : Entity
        where TWrapper : AzureObjectWrapper<TEntity>, TDefine
    {
        protected abstract TWrapper CreateObjectStore(TEntity entity);

        protected CloudTableClient _tableClient;
        protected string _entityTableName;

        protected AzureObjectStore(CloudTableClient tableClient, string entityTableName)
        {
            _tableClient = tableClient;
            _entityTableName = entityTableName;
        }

        #region Creation

        public virtual void Create(TEntity entity)
        {
            // Sanity check
            if (String.IsNullOrWhiteSpace(entity.RowKey))
            {
                throw new ArgumentException("entity.RowKey is empty", "entity.RowKey");
            }
            if (String.IsNullOrWhiteSpace(entity.PartitionKey))
            {
                throw new ArgumentException("entity.PartitionKey is empty", "entity.PartitionKey");
            }

            _tableClient.CreateTableIfNotExist(_entityTableName);
            var tableServiceContext = _tableClient.GetDataServiceContext();

            try
            {
                tableServiceContext.AddObject(_entityTableName, entity);
                tableServiceContext.SaveChanges();
            }
            catch (Exception ex)
            {
                if (ex.IsProblemResourceAlreadyExists())
                {
                    throw new DuplicateResourceException();
                }
                throw;
            }
        }

        #endregion

        #region Querying

        protected System.Data.Services.Client.DataServiceQuery<TEntity> Query
        {
            get
            {
                TableServiceContext tableServiceContext;
                return GetQuery(out tableServiceContext);
            }
        }

        protected System.Data.Services.Client.DataServiceQuery<TEntity> GetQuery(out TableServiceContext tableServiceContext)
        {
            tableServiceContext = _tableClient.GetDataServiceContext();
            tableServiceContext.IgnoreResourceNotFoundException = true;
            var query = tableServiceContext.CreateQuery<TEntity>(_entityTableName);
            return query;
        }

        public TDefine Find(string partitionKey, string rowKey)
        {
            var results = from entity in Query
                          where entity.RowKey == rowKey && entity.PartitionKey == partitionKey
                          select entity;
            try
            {
                foreach (var result in results)
                {
                    return CreateObjectStore(result);
                }
            }
            catch (System.Data.Services.Client.DataServiceQueryException)
            {

            }
            return default(TDefine);
        }

        public TDefine Find(Uri urn)
        {
            string partitionKey;
            var rowKey = urn.ParseRowKey(_tableClient, out partitionKey);
            return Find(partitionKey, rowKey);
        }

        public TDefine Find(Guid guid)
        {
            var rowKey = Entity.BuildRowKey(guid);
            var partitionKey = Entity.BuildPartitionKey(rowKey);
            return Find(partitionKey, rowKey);
        }

        public TDefine Find(DomainId id)
        {
            return Find(id.Guid);
        }

        public TDefine Find(string rowKey)
        {
            var results = from entity in Query
                          where entity.RowKey == rowKey
                          select entity;
            try
            {
                foreach (var result in results)
                {
                    return CreateObjectStore(result);
                }
            }
            catch (System.Data.Services.Client.DataServiceQueryException)
            {

            }
            return default(TDefine);
        }

        public IEnumerable<TDefine> All()
        {
            _tableClient.CreateTableIfNotExist(_entityTableName);
            var tableServiceContext = _tableClient.GetDataServiceContext();

            var query = tableServiceContext.CreateQuery<TEntity>(_entityTableName);
            var executedQuery = query.Execute();
            var results = from entity in executedQuery
                          select CreateObjectStore(entity);
            return results;
        }

        #endregion

        #region Referencing 
        
        public TWrapper GetReferencedObject(AzureObjectReference idRef)
        {
            if (idRef == null)
            {
                return default(TWrapper);
            }

            var tableServiceContext = _tableClient.GetDataServiceContext();
            tableServiceContext.IgnoreResourceNotFoundException = true;
            var query = tableServiceContext.CreateQuery<TEntity>(idRef.TableName);

            var results = from entity in query
                          where entity.RowKey == idRef.RowKey && entity.PartitionKey == idRef.PartitionKey
                          select entity;
            try
            {
                foreach (var result in results)
                {
                    return CreateObjectStore(result);
                }
            }
            catch (System.Data.Services.Client.DataServiceQueryException)
            {

            }
            return default(TWrapper);
        }

        protected IEnumerable<TWrapper> QueryOn<TModelObjectEntity>(
            JoshCodes.Web.Models.Persistence.IDefineModelObject referencedModelObject,
            Expression<Func<TEntity, string>> propertyExpr)
            where TModelObjectEntity : Entity
        {
            var idRef = referencedModelObject.GetAzureObjectReference<TModelObjectEntity>();
            var idRefEnc = Entity.Encode(idRef);
            var right = Expression.Constant(idRefEnc, typeof(string));
            var whereComparison = Expression.Equal(propertyExpr.Body, right);
            var whereCondition = Expression.Lambda<Func<TEntity, bool>>(whereComparison, propertyExpr.Parameters);

            MethodCallExpression whereCallExpression = Expression.Call(
                typeof(Queryable),
                "Where",
                new Type[] { typeof(TEntity) },
                Query.Expression,
                whereCondition);

            var query = Query.Provider.CreateQuery<TEntity>(whereCallExpression);
            foreach (var entity in query)
            {
                yield return CreateObjectStore(entity);
            }
        }

        #endregion

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
