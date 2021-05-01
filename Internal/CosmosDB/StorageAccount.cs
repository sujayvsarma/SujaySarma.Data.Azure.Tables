using System;

namespace SujaySarma.Data.Azure.Tables.Internal.CosmosDB
{
    /// <summary>
    /// An internal helper class to deal with Storage Account
    /// </summary>
    internal class StorageAccount
    {

        #region Properties

        /// <summary>
        /// If we are using the local development storage account
        /// </summary>
        public bool IsDevelopmentStorage
        {
            get;
            private set;

        } = false;


        /// <summary>
        /// Account name 
        /// </summary>
        public string AccountName
        {
            get;
            private set;

        }

        /// <summary>
        /// Storage account key
        /// </summary>
        public string AccountKey
        {
            get;
            private set;

        }

        /// <summary>
        /// Uri to the Table service (CosmosDB has the same scheme)
        /// </summary>
        public Uri TableUri
            => (IsDevelopmentStorage ? new Uri("http://127.0.0.1:10002/devstoreaccount1") : new Uri($"https://{AccountName}.{TableHostname}.{HostnameDomainName}/"));

        /// <summary>
        /// Recomposed connection string
        /// </summary>
        public string ConnectionString
            => $"DefaultEndpointsProtocol={(IsDevelopmentStorage ? "http" : "https")};AccountName={AccountName};AccountKey={AccountKey};EndpointSuffix=core.windows.net";

        #endregion

        #region Constructors

        /// <summary>
        /// Initialize the account information
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        public StorageAccount(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentException(connectionString);
            }

            IsDevelopmentStorage = connectionString.StartsWith(UseDevelopmentStorage);
            AccountName = string.Empty;
            AccountKey = string.Empty;

            if (IsDevelopmentStorage)
            {
                // we want to have things consistent below
                connectionString = DevelopmentStorageConnectionString;
                AccountName = DevelopmentStorageAccountName;
                AccountKey = DevelopmentStorageAccountKey;
            }

            foreach (string tokenSet in connectionString.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries))
            {
                // we are only interested in two tokens
                if (tokenSet.StartsWith("AccountName"))
                {
                    AccountName = tokenSet.Split(new char[] { '=' }, StringSplitOptions.None)[1];
                    continue;
                }

                if (tokenSet.StartsWith("AccountKey"))
                {
                    // AccountKey is base64 encoded and will have "==" at the end
                    AccountKey = tokenSet[(tokenSet.IndexOf('=') + 1)..];
                    continue;
                }

                if ((!string.IsNullOrWhiteSpace(AccountName)) && (!string.IsNullOrWhiteSpace(AccountKey)))
                {
                    break;
                }
            }

            if (string.IsNullOrWhiteSpace(AccountName))
            {
                throw new ArgumentException("AccountName is null or empty");
            }

            if (string.IsNullOrWhiteSpace(AccountKey))
            {
                throw new ArgumentException("AccountKey is null or empty");
            }
        }

        #endregion

        #region Private definitions

        private const string UseDevelopmentStorage = "UseDevelopmentStorage=true";
        private const string DevelopmentStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;EndpointSuffix=core.windows.net";
        private const string TableHostname = "table";
        private const string HostnameDomainName = "core.windows.net";
        private const string DevelopmentStorageAccountName = "devstoreaccount1";
        private const string DevelopmentStorageAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

        #endregion

    }
}
