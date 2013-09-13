using System;
using System.Linq;

using Microsoft.WindowsAzure.StorageClient;

namespace JoshCodes.Persistence.Azure.Storage
{
    public static class ReferenceExtensions
    {
        public static AzureObjectReference GetAzureObjectReference<TModelObjectEntity>(this JoshCodes.Web.Models.Persistence.IDefineModelObject modelObject)
            where TModelObjectEntity : Storage.Entity
        {
            if (!(modelObject is AzureObjectWrapper<TModelObjectEntity>))
            {
                throw new Exceptions.InvalidLinkedObjectException();
            }
            var azureModelObject = (AzureObjectWrapper<TModelObjectEntity>)modelObject;
            var reference = azureModelObject.GetAzureObjectReference();
            return reference;
        }

        public static TWrapper GetObject<TDefine, TWrapper, TEntity>(
            this AzureObjectReference idRef, AzureObjectStore<TDefine, TWrapper, TEntity> store)
            where TEntity : Entity
            where TWrapper : AzureObjectWrapper<TEntity>, TDefine 
        {
            if (idRef == null)
            {
                return default(TWrapper);
            }
            return store.GetReferencedObject(idRef);
        }
    }
}
