using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using JoshCodes.Persistence.Azure.Storage.Extensions;
using JoshCodes.Web.Models.Domain;
using Microsoft.WindowsAzure.Storage.Table;

namespace JoshCodes.Persistence.Azure.Storage
{
    public abstract class AzureObjectStore<TDefine, TWrapper, TEntity>
        where TEntity : Entity, new()
        where TWrapper : AzureObjectWrapper<TEntity>, TDefine
    {
        protected abstract TWrapper CreateObjectStore(TEntity entity);

        protected CloudTableClient _tableClient;
        protected string _entityTableName;

        protected AzureObjectStore(CloudTableClient tableClient, string entityTableName = null)
        {
            _tableClient = tableClient;
            _entityTableName = String.IsNullOrWhiteSpace(entityTableName)?
                typeof(TEntity).Name.ToLower() :
                entityTableName;
        }

        #region CUD Operations

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

            var table = _tableClient.GetTableReference(_entityTableName);
            table.CreateIfNotExists();

            try
            {

                TableOperation insertOperation = TableOperation.Insert(entity);
                table.Execute(insertOperation);

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

        //public void DeleteAll()
        //{
        //    TableServiceContext serviceContext;
        //    foreach (var entity in this.GetQuery(out serviceContext))
        //    {
        //        serviceContext.DeleteObject(entity);
        //    }
        //    serviceContext.SaveChanges();
        //}

        #endregion

        #region Querying

        protected TableQuery<TEntity> Query
        {
            get
            {
                CloudTable table;
                return GetQuery(out table);
            }
        }

        protected TableQuery<TEntity> GetQuery(out CloudTable table)
        {
            //tableServiceContext.IgnoreResourceNotFoundException = true;

            table = _tableClient.GetTableReference(_entityTableName);
            TableQuery<TEntity> query = (new TableQuery<TEntity>());

            return query;
        }

        public TDefine Find(string partitionKey, string rowKey)
        {

            var table = _tableClient.GetTableReference(_entityTableName);

            // Create a retrieve operation that takes a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<TEntity>(partitionKey, rowKey);

            // Execute the retrieve operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            // Check the result to make sure we found something
            if (retrievedResult.Result != null)
            {
                try
                {
                    return CreateObjectStore(((TEntity)retrievedResult.Result));
                }
                catch (System.Data.Services.Client.DataServiceQueryException)
                {

                }
            }

            return default(TDefine);
        }

        public TDefine Find(Uri urn)
        {
            string partitionKey;
            var rowKey = urn.ParseRowKey(_tableClient, out partitionKey);
            return Find(partitionKey, rowKey);
        }

        public virtual TDefine Find(Guid guid)
        {
            var rowKey = Entity.BuildRowKey(guid);
            var partitionKey = Entity.BuildPartitionKey(rowKey);
            var entity = Find(partitionKey, rowKey);
            if(!(entity == null))
            {
                return entity;
            }
            return default(TDefine);
        }

        public TDefine Find(DomainId id)
        {
            return Find(id.Guid);
        }

        public TDefine FindByHashedRowkey(string rowKey)
        {
            var paritionKey = Entity.BuildPartitionKey(rowKey);
            return Find(paritionKey, rowKey);
        }

        public TDefine Find(string rowKey)
        {
            // Create the CloudTable object that represents the "people" table.
            var table = _tableClient.GetTableReference(_entityTableName);
          
            // Construct the query operation for all customer entities where PartitionKey="Smith".
            TableQuery<TEntity> query = new TableQuery<TEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, rowKey));

            var results = table.ExecuteQuery(query);

            // Print the fields for each customer.

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
            var table = _tableClient.GetTableReference(_entityTableName);
            table.CreateIfNotExists();

            TableQuery<TEntity> query = (new TableQuery<TEntity>());

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

            //var tableServiceContext = _tableClient.GetDataServiceContext();
            //tableServiceContext.IgnoreResourceNotFoundException = true;
            //var query = tableServiceContext.CreateQuery<TEntity>(idRef.TableName);

            var table = _tableClient.GetTableReference(_entityTableName);
            table.CreateIfNotExists();

            TableQuery<TEntity> query = (new TableQuery<TEntity>());

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
            Web.Models.Persistence.IDefineModelObject referencedModelObject,
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
                new[] { typeof(TEntity) },
                Query.Expression,
                whereCondition);

            var query = Query.Provider.CreateQuery<TEntity>(whereCallExpression);
            foreach (var entity in query)
            {
                yield return CreateObjectStore(entity);
            }
        }

        #endregion
        
        private class AutoIncrementStorage : TableEntity
        {
            public long Value { private get; set; }
        }

        protected static long AutoIncrementedValue(CloudTableClient tableClient, string entityTableName, string partitionKey, string rowKey)
        {
            //var tableServiceContext = tableClient.GetDataServiceContext();
            do
            {
                //tableServiceContext.IgnoreResourceNotFoundException = true;
                //var query = tableServiceContext.CreateQuery<AutoIncrementStorage>(entityTableName);



                TableQuery<TEntity> query = (new TableQuery<TEntity>());

                var results = from entity in query
                              where entity.RowKey == rowKey && entity.PartitionKey == partitionKey
                              select entity;
                var resultsList = results.ToList();
                try
                {
                    long autoIncrementValue;
                    if (resultsList.Count() == 0)
                    {
                        var table = tableClient.GetTableReference(entityTableName);
                        table.CreateIfNotExists();

                        var storage = new AutoIncrementStorage
                        {
                            Value = 2,
                            RowKey = rowKey,
                            PartitionKey = partitionKey
                        };
                        autoIncrementValue = 1;
                        //tableServiceContext.AddObject(entityTableName, storage);

                        TableOperation mergeOperation = TableOperation.Merge(storage);
                        table.Execute(mergeOperation);

                    }
                    else
                    {
                        var storage = results.First();
                        autoIncrementValue = storage.Value;
                        storage.Value++;
                        //tableServiceContext.UpdateObject(storage);


                        var table = tableClient.GetTableReference(entityTableName);
                        TableOperation mergeOperation = TableOperation.Merge(storage);
                        table.Execute(mergeOperation);

                    }

                    //tableServiceContext.SaveChanges();
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
