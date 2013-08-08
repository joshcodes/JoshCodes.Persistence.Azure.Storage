using System;
using System.Linq;

using Microsoft.WindowsAzure.StorageClient;

using JoshCodes.Core.Urns.Extensions;
using JoshCodes.Web.Attributes.Extensions;

using JoshCodes.Persistence.Azure.Sql;
using System.Linq.Expressions;

namespace JoshCodes.Persistence.Azure.Sql.Extensions
{
    public static class Extensions
    {
        public static string ParseRowKey(this Uri uri, CloudTableClient tableClient, out string partitionKey)
        {
            string ns;
            string [] nss = uri.ParseUrnNamespaceString(out ns);
            var rowKey = nss[2];
            partitionKey = nss[1];
            return rowKey;
        }

        internal static Uri BuildUrn<T>(this AzureObjectWrapper<T> defines, string rowKey, string partitionKey, CloudTableClient tableClient)
            where T : TableServiceEntity
        {
            var nsId = defines.GetType().GetUrnNamespaceIdentifier(true);
            var urn = nsId.ToUrn(tableClient.BaseUri.Host, partitionKey, rowKey);
            return urn;
        }

        /// <summary>
        /// Get the status code of the response
        /// </summary>
        /// <param name="exception"></param>
        /// <returns>Status code for exception or -1 if response if unavailable</returns>
        public static int GetStatusCode(this System.Data.Services.Client.DataServiceRequestException exception)
        {
            System.Data.Services.Client.DataServiceResponse resp = exception.Response;

            int statusCode = -1;
            if (resp.IsBatchResponse)
            {
                statusCode = resp.BatchStatusCode;
            }
            else if (resp.Any())
            {
                statusCode = resp.First().StatusCode;
            }

            return statusCode;
        }

        // TODO: DRY this out

        public static bool IsProblemPreconditionFailed(this Exception exception)
        {
            var preconditionFailedCode = (int)System.Net.HttpStatusCode.PreconditionFailed;
            if(exception is System.Data.Services.Client.DataServiceRequestException)
            {
                var dsre = (System.Data.Services.Client.DataServiceRequestException)exception;
                int statusCode = dsre.GetStatusCode();
                return (statusCode == preconditionFailedCode);
            }
            if(exception is System.Data.Services.Client.DataServiceClientException)
            {
                var dsce = (System.Data.Services.Client.DataServiceClientException)exception;
                return (dsce.StatusCode == preconditionFailedCode);
            }
            if(exception is StorageClientException)
            {
                var sce = (StorageClientException)exception;
                return (sce.StatusCode == System.Net.HttpStatusCode.PreconditionFailed);
            }
            return false;
        }

        public static bool IsProblemResourceAlreadyExists(this Exception exception)
        {
            var conflictFailedCode = (int)System.Net.HttpStatusCode.Conflict;
            if (exception is System.Data.Services.Client.DataServiceRequestException)
            {
                var dsre = (System.Data.Services.Client.DataServiceRequestException)exception;
                int statusCode = dsre.GetStatusCode();
                return (statusCode == conflictFailedCode);
            }
            if (exception is System.Data.Services.Client.DataServiceClientException)
            {
                var dsce = (System.Data.Services.Client.DataServiceClientException)exception;
                return (dsce.StatusCode == conflictFailedCode);
            }
            if (exception is StorageClientException)
            {
                var sce = (StorageClientException)exception;
                return (sce.StatusCode == System.Net.HttpStatusCode.Conflict);
            }
            return false;
        }

        public static bool AtomicModification<TEntity, TValue>(
            this TEntity entity,
            TValue requiredValue, TValue newValue, out TValue currentValue,
            TableServiceContext tableServiceContext,
            Expression<Func<TEntity, TValue>> propertySelector)
            where TValue : IComparable<TValue>
            where TEntity : TableServiceEntity
        {
            // check the the object is in the requested state
            currentValue = propertySelector.Compile().Invoke(entity);
            if (requiredValue.CompareTo(currentValue) != 0)
            {
                return false;
            }

            var bodyType = propertySelector.Body.GetType();
            var propertyExpr = (System.Linq.Expressions.MemberExpression)propertySelector.Body;
            var memberInfo = (System.Reflection.PropertyInfo)propertyExpr.Member;
            memberInfo.SetValue(entity, newValue);

            try
            {
                tableServiceContext.UpdateObject(entity);
                tableServiceContext.SaveChanges();
                return true;
            }
            catch (Exception ex)
            {
                if (ex.IsProblemPreconditionFailed())
                {
                    return false;
                }
                throw;
            }
        }
    }
}
