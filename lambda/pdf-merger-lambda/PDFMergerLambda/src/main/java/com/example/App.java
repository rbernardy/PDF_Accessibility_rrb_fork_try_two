package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.pdfbox.multipdf.PDFMergerUtility;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;



/**
 * AWS Lambda function handler for merging PDFs stored in an S3 bucket.
 * This class implements the {@link RequestHandler} interface to process
 * requests containing PDF file names, merge those PDFs, and upload the result
 * back to S3.
 */
public class App implements RequestHandler<Map<String, Object>, String> {

    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

    /**
     * Handles the Lambda function request.
     *
     * @param input The input map containing the file names of PDFs to be merged.
     *              The map should have a key "fileNames" with a list of S3 object keys.
     * @param context The context object provides methods and properties that provide
     *                information about the invocation, function, and execution environment.
     * @return A message indicating the success or failure of the PDF merging process.
     */
    @Override
    @SuppressWarnings("unchecked")
    public String handleRequest(Map<String, Object> input, Context context) {
        String bucketName = System.getenv("BUCKET_NAME"); // Replace with your S3 bucket name

        // Extract the list of file names from the input
        List<String> pdfKeys = (List<String>) input.get("fileNames");
        if (pdfKeys == null || pdfKeys.isEmpty()) {
            return "No files to merge.";
        }
        
        List<String> modifiedPdfKeys = pdfKeys.stream()
            .map(key -> {
                int lastSlashIndex = key.lastIndexOf('/');
                if (lastSlashIndex != -1) {
                    String directory = key.substring(0, lastSlashIndex + 1); // Include the slash
                    String fileName = key.substring(lastSlashIndex + 1);
                    return directory + "FINAL_" + fileName;
                } else {
                    return "FINAL_" + key; // If no directory is found, prepend "FINAL_"
                }
            })
            .collect(Collectors.toList());

        // Extract the full path from first chunk to preserve folder structure
        String firstKey = pdfKeys.get(0);
        // Example: temp/folder-name/filename/filename_chunk_1.pdf
        
        // Get just the filename from the last segment
        String fullFileName = firstKey.substring(firstKey.lastIndexOf('/') + 1);
        String baseFileName = fullFileName.replaceAll("_chunk_\\d+", "");
        
        // Extract the directory path up to and including the filename folder
        // Example: temp/folder-name/filename/
        String directory = firstKey.substring(0, firstKey.lastIndexOf('/') + 1);

        String mergedFilePath = "/tmp/merged_" + baseFileName;
        String outputKey = directory + "merged_" + baseFileName;

        try {
            // Download PDFs from S3
            for (String key : modifiedPdfKeys) {
                downloadPDF(bucketName, key, baseFileName);
            }

            // Merge the PDFs
            mergePDFs(modifiedPdfKeys, mergedFilePath, baseFileName);
           
            // Upload merged PDF back to S3
            uploadPDF(bucketName, outputKey, mergedFilePath, baseFileName);
          
            // return "PDFs merged successfully and uploaded to: " + outputKey;

            return String.format("PDFs merged successfully.\nBucket: %s\nMerged File Key: %s\nMerged File Name: %s", 
                             bucketName, outputKey, baseFileName);
        } catch (Exception e) {
            baseFileName = baseFileName.replace(".pdf", "");
            System.out.println("File: " + baseFileName + ", Status: Failed in Merging the PDF");
            System.out.println(String.format("Filename: %s, File not found: %s", baseFileName, e.getMessage()));
            return "Failed to merge PDFs.";
        } finally {
            // Clean up temporary files to prevent "no space left on device" errors
            cleanupTempFiles(modifiedPdfKeys, mergedFilePath, baseFileName);
        }
    }

    /**
     * Downloads a PDF file from S3 to the local temporary directory.
     *
     * @param bucketName The name of the S3 bucket.
     * @param key The S3 object key of the PDF file to download.
     * @param baseFileName The base name of the file used for logging purposes.
     * @throws IOException If there is an issue downloading the file from S3.
     */
    private void downloadPDF(String bucketName, String key, String baseFileName) throws IOException {
        File localFile = new File("/tmp/" + key.substring(key.lastIndexOf('/') + 1));
        System.out.println(String.format("Filename: %s, Downloading file from S3: %s to %s", baseFileName, key, localFile.getPath()));
        s3Client.getObject(new GetObjectRequest(bucketName, key), localFile);
    }

    /**
     * Merges multiple PDF files into a single PDF file.
     *
     * @param sourceKeys The list of S3 object keys representing the PDF files to be merged.
     * @param destinationPath The file path where the merged PDF will be saved.
     * @param baseFileName The base name of the file used for logging purposes.
     * @throws IOException If there is an issue merging the PDF files.
     */
    private void mergePDFs(List<String> sourceKeys, String destinationPath, String baseFileName) throws IOException {
        PDFMergerUtility pdfMerger = new PDFMergerUtility();

        for (String key : sourceKeys) {
            String localFilePath = "/tmp/" + key.substring(key.lastIndexOf('/') + 1);
            File localFile = new File(localFilePath);

            if (localFile.exists()) {
                System.out.println(String.format("Filename: %s, Adding PDF to merge: %s", baseFileName, localFilePath));
                pdfMerger.addSource(localFile);
            } else {
                System.out.println(String.format("Filename: %s, File not found: %s", baseFileName, localFilePath));
            }
        }

        pdfMerger.setDestinationFileName(destinationPath);
        pdfMerger.mergeDocuments(null);
        System.out.println(String.format("Filename: %s, PDFs merged successfully into: %s", baseFileName, destinationPath));
    }

    /**
     * Uploads the merged PDF file back to S3.
     *
     * @param bucketName The name of the S3 bucket.
     * @param key The S3 object key for the uploaded merged PDF.
     * @param filePath The local file path of the merged PDF.
     * @param baseFileName The base name of the file used for logging purposes.
     */
    private void uploadPDF(String bucketName, String key, String filePath, String baseFileName) {
        System.out.println(String.format("Filename: %s, Uploading merged PDF to S3: %s as %s", baseFileName, filePath, key));
        s3Client.putObject(new PutObjectRequest(bucketName, key, new File(filePath)));
        logFileStatus(baseFileName);
    }

    /**
     * Logs the status of the file processing.
     *
     * @param baseFileName The base name of the file used for logging purposes.
     */
    private void logFileStatus(String baseFileName) {
        baseFileName = baseFileName.replace(".pdf", "");
        System.out.println(String.format("File: %s, Status: succeeded", baseFileName));
    }

    /**
     * Cleans up temporary files from /tmp directory to prevent disk space issues.
     *
     * @param sourceKeys The list of S3 object keys representing the downloaded PDF files.
     * @param mergedFilePath The path to the merged PDF file.
     * @param baseFileName The base name of the file used for logging purposes.
     */
    private void cleanupTempFiles(List<String> sourceKeys, String mergedFilePath, String baseFileName) {
        try {
            // Delete downloaded source files
            for (String key : sourceKeys) {
                String localFilePath = "/tmp/" + key.substring(key.lastIndexOf('/') + 1);
                File localFile = new File(localFilePath);
                if (localFile.exists() && localFile.delete()) {
                    System.out.println(String.format("Filename: %s, Deleted temp file: %s", baseFileName, localFilePath));
                }
            }
            
            // Delete merged file
            File mergedFile = new File(mergedFilePath);
            if (mergedFile.exists() && mergedFile.delete()) {
                System.out.println(String.format("Filename: %s, Deleted merged temp file: %s", baseFileName, mergedFilePath));
            }
        } catch (Exception e) {
            System.out.println(String.format("Filename: %s, Warning: Failed to cleanup temp files: %s", baseFileName, e.getMessage()));
        }
    }
}
