using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
