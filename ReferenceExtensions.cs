using System;

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
    }
}
