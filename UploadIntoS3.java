

import java.io.File;
import java.io.IOException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;

public class UploadIntoS3 {
	private static String bucketName     = "krazzybucket";
	private static String keyName        = "q2.jpg";
	private static String uploadFileName = "D:\\q2.jpg"; // D:\\Reading Zone\\MS\\Third Semester\\Cloud\\all_month.csv";
	static long startTime, endTime;
	
	public static void main(String[] args) throws IOException {
        AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());
        try {
          
        	System.out.println("Uploading a new file to S3 ..\n");
            File file = new File(uploadFileName);
            startTime = System.currentTimeMillis();
            PutObjectRequest por = new PutObjectRequest(bucketName, keyName, file);
            por.setCannedAcl(CannedAccessControlList.PublicRead);
            s3client.putObject(por);            
            endTime = System.currentTimeMillis();            
            System.out.println("Total time taken is :"+ (endTime - startTime)+ " milliseconds or "+(endTime - startTime)/1000 + " seconds.");
            
            
        	/*System.out.println("\nDownloading a file from s3..");
            startTime = System.currentTimeMillis();
            s3client.getObject(new GetObjectRequest("krazzybucket", "venkat"), new File("D:\\rajkumar.csv"));
            endTime = System.currentTimeMillis(); 
            System.out.println("Download time from AWS is "+(endTime - startTime)+ " milliseconds or "+(endTime - startTime)/1000+" seconds.");
         */
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
            		"means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which " +
            		"means the client encountered " +
                    "an internal error while trying to " +
                    "communicate with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }
}