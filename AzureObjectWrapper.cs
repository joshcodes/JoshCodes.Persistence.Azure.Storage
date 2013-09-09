using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Xml;
using System.IO;
using System.Runtime.Serialization;
using System.Net;
using System.Data.Services.Client;

using Microsoft.WindowsAzure.StorageClient;

using JoshCodes.Persistence.Azure.Storage.Extensions;

namespace JoshCodes.Persistence.Azure.Storage
{
    public class AzureObjectWrapper<TEntity> where TEntity : Entity
    {
        // Information required to access the entity object
        private string _rowKey;
        private string _partitionKey;

        // Where the entity object is accessed from
        protected CloudTableClient _tableClient;
        private string _entityTableName;

        // The all important entity object which is null
        // until needed
        private TEntity _storage;
        
        #region Constructors

        // Load rowkey and partition key from URN
        public AzureObjectWrapper(Uri urn, CloudTableClient tableClient, string entityTableName)
        {
            _rowKey = urn.ParseRowKey(tableClient, out _partitionKey);
            _tableClient = tableClient;
            _entityTableName = entityTableName;
        }

        // Provide row and partition key directly
        public AzureObjectWrapper(string key, string partitionKey, CloudTableClient tableClient, string entityTableName)
        {
            this._rowKey = key;
            this._partitionKey = partitionKey;
            this._tableClient = tableClient;
            this._entityTableName = entityTableName;
        }

        // Initialize with storage object already accessed
        public AzureObjectWrapper(TEntity storage, CloudTableClient tableClient, string entityTableName)
        {
            this._storage = storage;
            this._rowKey = storage.RowKey;
            this._partitionKey = storage.PartitionKey;
            this._tableClient = tableClient;
            this._entityTableName = entityTableName;
        }

        // Initialize and create a storage object (this one's a little weird and should
        // probably be in Object Store.
        public delegate TEntity CreateEntity(out string partitionKey, out string rowKey);

        public AzureObjectWrapper(CloudTableClient tableClient, string entityTableName, CreateEntity createEntity)
        {
            this._tableClient = tableClient;
            this._entityTableName = entityTableName;

            tableClient.CreateTableIfNotExist(entityTableName);
            var tableServiceContext = tableClient.GetDataServiceContext();

            this._storage = createEntity.Invoke(out this._partitionKey, out this._rowKey);
            this._storage.PartitionKey = _partitionKey;
            this._storage.RowKey = _rowKey;

            tableServiceContext.AddObject(entityTableName, this._storage);
            tableServiceContext.SaveChanges();
        }

        #endregion

        protected TEntity Storage
        {
            get
            {
                if (_storage == null)
                {
                    this.EditableStorage((s) => { _storage = s; return true; });
                }
                return _storage;
            }
        }

        #region Properties

        public Guid IdGuid
        {
            get
            {
                Guid guid;
                if (System.Guid.TryParse(_rowKey, out guid))
                {
                    return guid;
                }
                if (System.Guid.TryParse(this.Storage.IdGuid, out guid))
                {
                    return guid;
                }
                return Guid.Empty;
            }
        }

        public string IdKey
        {
            get { return _rowKey; }
        }

        public Uri IdUrn
        {
            get
            {
                return this.BuildUrn(_rowKey, _partitionKey, _tableClient);
            }
        }

        protected string Partition
        {
            get { return _partitionKey; }
        }

        #endregion

        #region Mutators

        protected bool AtomicModification<T>(T requiredValue, T newValue, out T currentValue, Expression<Func<TEntity, T>> propertySelector)
            where T : IComparable<T>
        {
            var tableServiceContext = _tableClient.GetDataServiceContext();
            var query = tableServiceContext.CreateQuery<TEntity>(_entityTableName);
            var results = from entity in query
                          where entity.RowKey == _rowKey && entity.PartitionKey == _partitionKey
                          select entity;
            var storage = results.First();

            var success = storage.AtomicModification(requiredValue, newValue, out currentValue, tableServiceContext, propertySelector);
            if (success)
            {
                this._storage = storage;
            }
            return success;
        }

        protected bool AtomicModification(
            Func<TEntity, bool> conditionForExecution,
            Action<TEntity> updateAction,
            Action<TEntity> onSuccess)
        {
            var tableServiceContext = _tableClient.GetDataServiceContext();
            var query = tableServiceContext.CreateQuery<TEntity>(_entityTableName);
            var results = from entity in query
                          where entity.RowKey == _rowKey && entity.PartitionKey == _partitionKey
                          select entity;
            var storage = results.First();

            var success = storage.AtomicModification(conditionForExecution, updateAction, onSuccess, tableServiceContext);
            if (success)
            {
                this._storage = storage;
            }
            return success;
        }

        protected void EditableStorage(Func<TEntity, bool> callback)
        {
            this.EditableStorage((entity, isRetry) =>
                {
                    return callback.Invoke(entity);
                });
        }

        protected void EditableStorage(Func<TEntity, bool, bool> callback)
        {
            bool isPreconditionFailedResponse = false;
            var tableServiceContext = _tableClient.GetDataServiceContext();
            do
            {
                var query = tableServiceContext.CreateQuery<TEntity>(_entityTableName);
                var results = from entity in query
                              where entity.RowKey == _rowKey && entity.PartitionKey == _partitionKey
                              select entity;
                var storage = results.First();

                try
                {
                    if (callback.Invoke(storage, isPreconditionFailedResponse))
                    {
                        tableServiceContext.UpdateObject(storage);
                        tableServiceContext.SaveChanges();
                    }
                    return;
                }
                catch (Exception ex)
                {
                    isPreconditionFailedResponse = ex.IsProblemPreconditionFailed();
                    if (!isPreconditionFailedResponse)
                    {
                        throw;
                    }
                }
            } while (!isPreconditionFailedResponse);
        }

        public void Delete()
        {
            var tableServiceContext = _tableClient.GetDataServiceContext();
            var query = tableServiceContext.CreateQuery<TEntity>(_entityTableName);
            var results = from entity in query
                          where entity.RowKey == _rowKey && entity.PartitionKey == _partitionKey
                          select entity;
            
            var storage = results.First();
            tableServiceContext.DeleteObject(storage);
            tableServiceContext.SaveChanges();
        }

        #endregion
        
        #region Referenced objects

        [DataContract(Name="r")]
        private class IdReference
        {
            public IdReference(string rowKey, string partitionKey, string tableName)
            {
                this.RowKey = rowKey;
                this.PartitionKey = partitionKey;
                this.TableName = tableName;
            }

            [DataMember(Name="k")]
            public string RowKey { get; set; }

            [DataMember(Name = "p")]
            public string PartitionKey { get; set; }
            
            [DataMember(Name = "t")]
            public string TableName { get; set; }
        }

        protected delegate TWrapper GetWrapperFromEntityDelegate<TEntity, TWrapper>(TEntity entity)
            where TEntity : Entity
            where TWrapper : AzureObjectWrapper<TEntity>;

        protected TWrapper GetReferencedObject<TEntity, TWrapper>(string id, GetWrapperFromEntityDelegate<TEntity, TWrapper> converter)
            where TEntity : Entity
            where TWrapper : AzureObjectWrapper<TEntity>
        {
            var idReference = Decode<IdReference>(id);

            var tableServiceContext = _tableClient.GetDataServiceContext();
            tableServiceContext.IgnoreResourceNotFoundException = true;
            var query = tableServiceContext.CreateQuery<TEntity>(idReference.TableName);
            
            var results = from entity in query
                          where entity.RowKey == idReference.RowKey && entity.PartitionKey == idReference.PartitionKey
                          select entity;
            try
            {
                foreach (var result in results)
                {
                    return converter(result);
                }
            }
            catch (System.Data.Services.Client.DataServiceQueryException)
            {

            }
            return default(TWrapper);
        }

        protected string SetReferencedObject<TEntity>(AzureObjectWrapper<TEntity> obj)
            where TEntity : Entity
        {
            var reference = new IdReference(obj._rowKey, obj._partitionKey, obj._entityTableName);
            var encodedReference = Encode<IdReference>(reference);
            return encodedReference;
        }

        #endregion

        #region Utility methods

        protected static string GetBaseUriString(CloudTableClient tableClient, string entityTableName)
        {
            return tableClient.BaseUri.AbsoluteUri + "/" + entityTableName + "/";
        }

        protected static string ParseRowKey(Uri id, CloudTableClient tableClient, string entityTableName)
        {
            var rowKey = id.AbsoluteUri.Substring(GetBaseUriString(tableClient, entityTableName).Length);
            return rowKey;
        }

        protected static T Decode<T>(string value)
        {
            if (String.IsNullOrWhiteSpace(value))
            {
                return default(T);
            }
            var reader = XmlReader.Create(new StringReader(value));
            var serializer = new DataContractSerializer(typeof(T));
            T result = (T)serializer.ReadObject(reader);
            return result;
        }

        protected static string Encode<T>(T value)
        {
            if (EqualityComparer<T>.Default.Equals(value))
            {
                return String.Empty;
            }
            string serializedString;
            using (MemoryStream memoryStream = new MemoryStream())
            using (StreamReader reader = new StreamReader(memoryStream))
            {
                DataContractSerializer serializer = new DataContractSerializer(typeof(T));
                serializer.WriteObject(memoryStream, value);
                memoryStream.Position = 0;
                serializedString = reader.ReadToEnd();
            }
            return serializedString;
        }

        #endregion
    }
}
