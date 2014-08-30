using System;

namespace JoshCodes.Persistence.Azure.Storage.Exceptions
{
    public class InvalidLinkedObjectException : Exception
    {
        public InvalidLinkedObjectException()
            : base("The referenced object is not from this persistence layer")
        {
        }
    }
}
