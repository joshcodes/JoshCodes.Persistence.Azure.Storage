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
        public AzureObjectWrapper(Guid key, CloudTableClient tableClient, string entityTableName)
        {
            _rowKey = Entity.BuildRowKey(key);
            _partitionKey = Entity.BuildPartitionKey(_rowKey);
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
            if (String.IsNullOrWhiteSpace(this._rowKey))
            {
                throw new ArgumentOutOfRangeException("Storage row key is null");
            }

            this._partitionKey = storage.PartitionKey;
            if (String.IsNullOrWhiteSpace(this._rowKey))
            {
                throw new ArgumentOutOfRangeException("Storage partition key is null");
            }

            this._tableClient = tableClient;
            this._entityTableName = entityTableName;
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

        public Guid Key
        {
            get
            {
                return this.Storage.GetKey();
            }
        }

        public DateTime LastModified
        {
            get
            {
                return Storage.LastModified;
            }
            set
            {
                EditableStorage((item) =>
                {
                    item.LastModified = value;
                    return true;
                });
            }
        }

        public string[] UrnNamespace
        {
            get
            {
                return new string[] { this._partitionKey, this._rowKey };
            }
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

        public AzureObjectReference GetAzureObjectReference()
        {
            var reference = new AzureObjectReference(this._rowKey, this._partitionKey, this._entityTableName);
            return reference;
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
