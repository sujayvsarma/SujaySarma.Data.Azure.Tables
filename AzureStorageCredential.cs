namespace SujaySarma.Data.Azure.Tables
{
    /// <summary>
    /// Represents a credential used to connect to Azure Storage
    /// </summary>
    public sealed class AzureStorageCredential
    {

        /// <summary>
        /// The AccountName used to connect
        /// </summary>
        public string AccountName { get; set; }

        /// <summary>
        /// The account Secret or Key used to authenticate
        /// </summary>
        public string AccountSecret { get; set; }


        /// <summary>
        /// Initialize using defaults (Development storage settings are used)
        /// </summary>
        public AzureStorageCredential()
        {
            AccountName = DevelopmentStorageAccountName;
            AccountSecret = DevelopmentStorageAccountKey;
        }

        /// <summary>
        /// Initialize using the provided values
        /// </summary>
        /// <param name="accountName">AccountName used to connect</param>
        /// <param name="accountKey">AccountKey or secret used to authenticate</param>
        public AzureStorageCredential(string accountName, string accountKey)
        {
            AccountName = accountName;
            AccountSecret = accountKey;
        }

        /// <summary>
        /// Returns an instance populated with values found in a connection string
        /// </summary>
        /// <param name="connectionString">Connectionstring to process</param>
        /// <returns>AzureStorageCredential</returns>
        public static AzureStorageCredential FromConnectionString(string connectionString)
        {
            AzureStorageCredential instance = new();

            if (! connectionString.Equals(UseDevelopmentStorage))
            {
                instance.AccountName = string.Empty;
                instance.AccountSecret = string.Empty;

                foreach (string tokenSet in connectionString.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries))
                {
                    // we are only interested in two tokens
                    if (tokenSet.StartsWith("AccountName"))
                    {
                        instance.AccountName = tokenSet.Split(new char[] { '=' }, StringSplitOptions.None)[1];
                        continue;
                    }

                    if (tokenSet.StartsWith("AccountKey"))
                    {
                        // AccountKey is base64 encoded and will have "==" at the end
                        instance.AccountSecret = tokenSet[(tokenSet.IndexOf('=') + 1)..];
                        continue;
                    }

                    if ((!string.IsNullOrWhiteSpace(instance.AccountName)) && (!string.IsNullOrWhiteSpace(instance.AccountSecret)))
                    {
                        break;
                    }
                }

                if (string.IsNullOrWhiteSpace(instance.AccountName) || string.IsNullOrWhiteSpace(instance.AccountSecret))
                {
                    throw new ArgumentException($"AccountName or AccountKey not found in {nameof(connectionString)}");
                }
            };            

            return instance;
        }


        /// <summary>
        /// Returns a connectionstring compatible serialization of the values
        /// </summary>
        /// <returns>String containing the AccountName and Secret</returns>
        public override string ToString()
        {
            return $"AccountName={AccountName};AccountKey={AccountSecret}";
        }

        private const string UseDevelopmentStorage = "UseDevelopmentStorage=true";
        private const string DevelopmentStorageAccountName = "devstoreaccount1";
        private const string DevelopmentStorageAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    }
}
