using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Build.Utilities;
using System.Threading.Tasks;
using System.Threading;
using System.IO.Compression;
using System.Text.RegularExpressions;

namespace UploadPackageOnBlob
{
    public static class Function1
    {
        [FunctionName("Function1")]
        public static void Run(
                [BlobTrigger("ddmfincoming/{name}.zip", Connection = "StorageConnectionString")]Stream myBlob, string name, ILogger log,
                [Blob("ddmfarchive/{name}.zip", FileAccess.Write, Connection = "StorageConnectionString")] TextWriter validationOutput)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
            validationOutput.WriteLine("Hello World");
            //CloudStorageAccount sourceAccount = CloudStorageAccount.Parse(Connection);



            string sourceContainer = "ddmfincoming";
            string destContainer = "ddmfpackage";
            if (!ValidateName(name))
            {

                log.LogWarning(">>>>>>>>>>>>>>>>>");
            }   
            else
            {
                string connectionString = System.Environment.GetEnvironmentVariable("StorageConnectionString");
                log.LogInformation(connectionString);
                CloudStorageAccount sourceAccount = CloudStorageAccount.Parse(connectionString);
                CloudBlobClient sourceClient = sourceAccount.CreateCloudBlobClient();
                CloudBlobContainer sourceBlobContainer = sourceClient.GetContainerReference(sourceContainer);
                CloudBlobContainer destBlobContainer = sourceClient.GetContainerReference(destContainer);
                ICloudBlob blob = sourceBlobContainer.GetBlockBlobReference(name);
                log.LogInformation(">>>>>>>>>" + blob.Name);                
                MoveBlobAsync(blob, sourceBlobContainer, destBlobContainer).Wait();

                Console.WriteLine("\nDone -- Begin to Zip file");

                CloudBlockBlob unzipBlob = destBlobContainer.GetBlockBlobReference(name);

                string unzipBlobName = name.Replace("_","").Replace("-","").ToLower();

                Console.WriteLine("Blob Unzip Name:" + unzipBlobName);

                Unzip(unzipBlob, sourceClient, unzipBlobName).Wait();
             
            }
        }
        public static async System.Threading.Tasks.Task Unzip(CloudBlockBlob blockBlob, CloudBlobClient sourceClient,string unzipContainerName)
        {
            CloudBlobContainer container = sourceClient.GetContainerReference(unzipContainerName);
            await container.CreateIfNotExistsAsync();

            using (var zipBlobFileStream = new MemoryStream())
            {
                await blockBlob.DownloadToStreamAsync(zipBlobFileStream);
                await zipBlobFileStream.FlushAsync();
                zipBlobFileStream.Position = 0;
                //use ZipArchive from System.IO.Compression to extract all the files from zip file
                using (var zip = new ZipArchive(zipBlobFileStream))
                {
                    //Each entry here represents an individual file or a folder
                    foreach (var entry in zip.Entries)
                    {
                        //creating an empty file (blobkBlob) for the actual file with the same name of file
                        var blob = container.GetBlockBlobReference(entry.FullName);
                        using (var stream = entry.Open())
                        {
                            //check for file or folder and update the above blob reference with actual content from stream
                            if (entry.Length > 0)
                                await blob.UploadFromStreamAsync(stream);
                        }

                        // TO-DO : Process the file (Blob)
                        //process the file here (blob) or you can write another process later 
                        //to reference each of these files(blobs) on all files got extracted to other container.
                    }
                }
            }

        }

        private static async System.Threading.Tasks.Task MoveBlobAsync(ICloudBlob sourceBlobRef, CloudBlobContainer sourceContainer, CloudBlobContainer destContainer)
        {
            CloudBlockBlob destBlob = destContainer.GetBlockBlobReference(sourceBlobRef.Name);
            await destBlob.StartCopyAsync(new Uri(GetShareAcccessUri(sourceBlobRef.Name+".zip", sourceContainer)));
            
            ICloudBlob destBlobRef = await destContainer.GetBlobReferenceFromServerAsync(sourceBlobRef.Name);
            while (destBlobRef.CopyState.Status == CopyStatus.Pending)
            {
                Console.WriteLine($"Blob: {destBlobRef.Name}, Copied: {destBlobRef.CopyState.BytesCopied ?? 0} of  {destBlobRef.CopyState.TotalBytes ?? 0}");
                await System.Threading.Tasks.Task.Delay(500);
                destBlobRef = await destContainer.GetBlobReferenceFromServerAsync(sourceBlobRef.Name);
            }
            Console.WriteLine($"Blob: {destBlob.Name} Complete");

            // Remove the source blob
            bool blobExisted = await sourceBlobRef.DeleteIfExistsAsync();


        }
        private static string GetShareAcccessUri(string blobName, CloudBlobContainer container)
        {
            DateTime toDateTime = DateTime.Now.AddMinutes(60);
            SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy
            {
                Permissions = SharedAccessBlobPermissions.Read,
                SharedAccessStartTime = null,
                SharedAccessExpiryTime = new DateTimeOffset(toDateTime)
            };
            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
            string sas = blob.GetSharedAccessSignature(policy);

            string uri = blob.Uri.AbsoluteUri + sas;
            Console.WriteLine(">>>>>>>>>>>>>>>>>>>>" + uri);
            return uri;


        }


        public static bool ValidateName(string name)
        {
            return true;
        }
    }
}
