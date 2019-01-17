
namespace PlatformSupport.DatabaseAccess
{
    public struct RecordIdentifier
    {
        private readonly long _identifier;

        public static readonly RecordIdentifier UnknownRecordIdentifier = new RecordIdentifier(-1);

        internal RecordIdentifier(long id)
        {
            _identifier = id;
        }

        /// <summary>
        /// This temporary implicit casting operator lets us define our methods as returning RecordIdentifier
        /// but allows callers to continue to put the identifier into a long. The intent is to remove the
        /// operator completely once callers are all converted.
        /// </summary>
        public static implicit operator long(RecordIdentifier id)
        {
            return id._identifier;
        }

        /// <summary>
        /// This temporary implicit casting operator provides a conversion that lets callers pass a long
        /// into a method that takes an identifier as a parameter.
        /// </summary>
        public static implicit operator RecordIdentifier(long id)
        {
            return new RecordIdentifier(id);
        }

        public RecordIdentifier Increment()
        {
            return new RecordIdentifier(_identifier + 1);
        }
    }

}