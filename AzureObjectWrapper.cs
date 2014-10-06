using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Xml;
using System.IO;
using System.Runtime.Serialization;
using JoshCodes.Persistence.Azure.Storage.Extensions;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Blob;

namespace JoshCodes.Persistence.Azure.Storage
{
    public class AzureObjectWrapper<TEntity> where TEntity : Entity
    {
        // Information required to access the entity object
        private string _rowKey;
        private string _partitionKey;

        // Where the entity object is accessed from
        protected CloudTableClient _tableClient;
        protected CloudBlobClient _blobClient;
        private string _entityTableName;

        // The all important entity object which is null
        // until needed
        private TEntity _storage;
        
        #region Constructors

        // Load rowkey and partition key from URN
        public AzureObjectWrapper(Guid key, CloudTableClient tableClient, string entityTableName = null)
        {
            _rowKey = Entity.BuildRowKey(key);
            _partitionKey = Entity.BuildPartitionKey(_rowKey);
            _tableClient = tableClient;
            _entityTableName = String.IsNullOrWhiteSpace(entityTableName)?
                typeof(TEntity).Name.ToLower() :
                entityTableName;;
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
        public AzureObjectWrapper(TEntity storage, CloudTableClient tableClient, string entityTableName = null)
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
            this._entityTableName = String.IsNullOrWhiteSpace(entityTableName)?
                typeof(TEntity).Name.ToLower() :
                entityTableName;
        }

        #endregion

        protected TEntity Storage
        {
            get
            {
                if (_storage == null)
                {
                    this.Save((s) => { _storage = s; return true; });
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

        public DateTimeOffset LastModified
        {
            get
            {
                return Storage.Timestamp;
            }
            set
            {
                Save((item) =>
                {
                    item.Timestamp = value;
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

            var table = _tableClient.GetTableReference(_entityTableName);

            // Create a retrieve operation that takes a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<TEntity>(_partitionKey, _rowKey);

            // Execute the retrieve operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            var storage = (TEntity)retrievedResult.Result;

            var success = storage.AtomicModification(requiredValue, newValue, out currentValue, _tableClient, propertySelector);
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
            var table = _tableClient.GetTableReference(_entityTableName);

            // Create a retrieve operation that takes a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<TEntity>(_partitionKey, _rowKey);

            // Execute the retrieve operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            var storage = (TEntity)retrievedResult.Result;

            var success = storage.AtomicModification(conditionForExecution, updateAction, onSuccess, _tableClient);

            if (success)
            {
                this._storage = storage;
            }
            return success;
        }

        // TODO: Change this to return null entity if do not save
        protected void Save(Func<TEntity, bool> callback)
        {
            this.EditableStorage((entity, isRetry) =>
                {
                    return callback.Invoke(entity);
                });
        }

        protected void EditableStorage(Func<TEntity, bool, bool> callback)
        {
            var isPreconditionFailedResponse = false;
            var table = _tableClient.GetTableReference(_entityTableName);
            do
            {
                try
                {
                    var mergeOperation = TableOperation.InsertOrMerge(_storage);
                    table.Execute(mergeOperation);
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
            var table = _tableClient.GetTableReference(_entityTableName);

            // Create a retrieve operation that takes a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<TEntity>(_partitionKey, _rowKey);

            // Execute the retrieve operation.
            TableResult retrievedResult = table.Execute(retrieveOperation);

            // Check the result to make sure we found something
            if (retrievedResult.Result != null)
            {
                TableOperation insertOperation = TableOperation.Delete((TEntity)retrievedResult.Result);
                table.Execute(insertOperation);
            }
        }

        public virtual bool Validate()
        {
            return true;
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
