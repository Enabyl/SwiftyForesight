//
//  CloudManager.swift
//  SwiftyForesight
//
//  Created by Jonathan Zia on 9/26/18.
//  Copyright © 2018 Enabyl Inc. All rights reserved.
//

import Foundation
import AWSCore
import AWSS3
import AWSDynamoDB

// This file contains functions for data transfer to and from remote locations.
// This includes:   (1) Upload/Download functionality with AWS S3 buckets
//                  (2) Uploading metadata to AWS DynamoDB databases

// Data upload and download are handled by the CloudManager class.
// This class manages the link between the mobile device and the server.

// The CloudManager class handles data transfer to and from AWS S3 buckets.
public class CloudManager {
    
    // Initialize attributes
    public var writeBucket: String  // S3 bucket name for *writing* data
    public var readBucket: String   // S3 bucket name for *reading* data
    public var identityID: String   // Identity ID for AWS permissions
    public var userID: String       // User ID (application user)
    
    public init(identityID iid: String, userID uid: String, writeBucket wb: String? = nil, readBucket rb: String? = nil) {
        
        // Write attribute values
        self.writeBucket = wb ?? "N/A"
        self.readBucket = rb ?? "N/A"
        self.identityID = iid
        self.userID = uid
        
    }
    
    // Define function for uploading file to S3 bucket
    public func uploadFile(Local localFilepath: URL, Remote remoteName: String, completion: ((Bool) -> Void)? = nil) {
        
        // Create placeholder for data to be uploaded
        var uploadData: Data
        
        // Ensure a write bucket has been specified
        guard self.writeBucket != "N/A" else {
            // Print error message and return completion
            print("Error in CloudManager.uploadFile(): writeBucket attribute not initialized")
            completion?(false); return
        }
        
        // If there is a file available, read it in to a data object
        do {
            uploadData = try Data(contentsOf: localFilepath)
        } catch {
            // Print error message and return completion
            print("Error in CloudManager.uploadFile(): Unable to locate data in specified filepath")
            completion?(false); return
        }
        
        // Create upload expression
        let expression = AWSS3TransferUtilityUploadExpression()
        
        // Keep track of transfer progress
        expression.progressBlock = {(task, progress) in
            DispatchQueue.main.async(execute: {
                // Print update (for debugging)
                print("Message from CloudManager.uploadFile(): Uploading file...")
            })
        }
        
        // Define completion handler
        var completionHandler: AWSS3TransferUtilityUploadCompletionHandlerBlock?
        completionHandler = {(task, error) -> Void in
            DispatchQueue.main.async(execute: {
                // Alert user of transfer status
                if let error = error {
                    // Print error and return completion
                    print("Error in CloudManager.uploadFile(): \(error)"); completion?(false)
                } else {
                    // Print message and return completion
                    print("Message from CloudManager.uploadFile(): Upload successful"); completion?(true)
                }
            })
            
        }
        
        // Define transfer utility as default
        let transferUtility = AWSS3TransferUtility.default()
        
        // Perform data transfer
        transferUtility.uploadData(uploadData, bucket: self.writeBucket, key: remoteName, contentType: "text/csv", expression: expression, completionHandler: completionHandler).continueWith {
            (task) -> AnyObject? in
            
            // If there is an error, print an error message
            if let error = task.error {
                print("Error in CloudManager.uploadFile(): \(error.localizedDescription)")
            }
            
            // If there is no error, alert the user that the transfer is being initialized
            if let _ = task.result {
                print("Message from CloudManager.uploadFile(): Initializing transfer")
            }
            
            return nil;
        }
    }
    
    // Define function for downloading file from S3 bucket
    public func downloadFile(Remote remoteName: String, Local localFilepath: URL, completion: ((Bool) -> Void)? = nil) {
        
        // Ensure a read bucket has been specified
        guard self.readBucket != "N/A" else {
            // Print error message and return completion
            print("Error in CloudManager.downloadFile(): readBucket attribute not initialized")
            completion?(false); return
        }
        
        // Ensure that there isn't already a file in the local filepath
        guard !FileManager.default.fileExists(atPath: localFilepath.path) else {
            // Print error message and return completion
            print("Error in CloudManager.downloadFile(): File already exists in destination path")
            completion?(false); return
        }
        
        // Create download expression
        let expression = AWSS3TransferUtilityDownloadExpression()
        
        // Keep track of transfer progress
        expression.progressBlock = {(task, progress) in
            DispatchQueue.main.async(execute: {
                // Print update (for debugging)
                print("Message from CloudManager.downloadFile(): Downloading file...")
            })
        }
        
        // Define completion handler
        var completionHandler: AWSS3TransferUtilityDownloadCompletionHandlerBlock?
        completionHandler = {(task, URL, data, error) -> Void in
            DispatchQueue.main.async(execute: {
                // Alert the user of transfer status
                if let error = error {
                    // Print error and return completion
                    print("Error in CloudManager.downloadFile(): \(error)"); completion?(false)
                } else {
                    // Print message and return completion
                    print("Message from CloudManager.downloadFile(): Download successful"); completion?(true)
                }
            })
        }
        
        // Define transfer utility as default
        let transferUtility = AWSS3TransferUtility.default()
        
        // Perform data transfer
        transferUtility.download(to: localFilepath, bucket: self.readBucket, key: remoteName, expression: expression, completionHandler: completionHandler).continueWith {
            (task) -> AnyObject? in
            
            // If there is an error, print an error message
            if let error = task.error {
                print("Error in CloudManager.downloadFile(): \(error.localizedDescription)")
            }
            
            // If there is no error, alert the user that the transfer is being initialized
            if let _ = task.result {
                print("Message from CloudManager.downloadFile(): Initializing transfer")
            }
            
            return nil
        }
        
    }
    
    // Define function for removing all user files from S3 bucket
    public func removeAllUserFiles(forUser user: String, completion: ((Bool) -> Void)? = nil) {
        
        // Set credentials provider and configuration
        let credentialsProvider = AWSCognitoCredentialsProvider(regionType: AWSRegionType.USEast1, identityPoolId: identityID)
        let configuration = AWSServiceConfiguration(region: AWSRegionType.USEast1, credentialsProvider: credentialsProvider)
        
        // Initialize AWS S3 manager with configuration
        AWSServiceManager.default().defaultServiceConfiguration = configuration
        AWSS3.register(with: configuration!, forKey: "defaultKey")
        let s3 = AWSS3.s3(forKey: "defaultKey")
        
        // Create delete object request and set bucket name
        let deleteRequest = AWSS3DeleteObjectRequest()
        deleteRequest?.bucket = self.writeBucket
        
        // Create list objecs request for objects with specified prefix
        let listRequest = AWSS3ListObjectsRequest()
        listRequest?.bucket = self.writeBucket
        listRequest?.prefix = user
        
        // List objects
        s3.listObjects(listRequest!).continueWith { (task: AWSTask) -> AnyObject? in
            
            // If there is an error, print notification and return completion
            if let error = task.error {
                print("Error in CloudManager.removeAllUserFiles(): \(error)"); completion?(false)
                
                // Else, continue with file removal
            } else if let list = task.result {
                
                // Check whether list has contents; if not, return empty set
                let listContents = list.contents ?? []
                
                // For the list of items returnd by the list request...
                for item in listContents {
                    
                    // Obtain name of each item in list
                    deleteRequest?.key = item.key
                    // Delete item
                    s3.deleteObject(deleteRequest!).continueWith { (subtask: AWSTask) -> AnyObject? in
                        
                        // If there is an error, print the error message and return completion
                        if let error = subtask.error {
                            print("Error in CloudManager.removeAllUserFiles(): \(error)"); completion?(false)
                        }
                        return nil
                    }
                    
                }
                
                // Return completion
                completion?(true)
                
            }
            return nil
        }
        
    }
    
    // Define function for uploading metadata to AWS DynamoDB
    public func uploadMetadata(forDictionary dict: [String:String], completion: ((Bool) -> Void)? = nil) {
        
        // Ensure that dictionary contains values for hash and range keys
        // Hash Key
        guard dict["_userID"] != nil else {
            // Print error and return completion
            print("Error in CloudManager.uploadMetadata(): No value for _userID attribute")
            completion?(false); return
        }
        
        // Range Key
        guard dict["_eventDate"] != nil else {
            // Print error and return completion
            print("Error in CloudManager.uploadMeta(): No value for _eventDate attribute")
            completion?(false); return
        }
        
        // Create DynamoDB Object Mapper
        let mapper = AWSDynamoDBObjectMapper.default()
        
        // Create a data item for organizing data for upload
        let dataItem = DatabaseClass()
        
        // Write metadata items to DatabaseClass
        dataItem?._userID = dict["_userID"]         // Hash Key
        dataItem?._eventDate = dict["_eventDate"]   // Range Key
        
        // -----------------------------------------------------------------
        // EDIT CODE BELOW ONLY
        // -----------------------------------------------------------------
        
        // MARK: EDIT
        // If there are other metadata items in your DatabaseClass, populate them here
        // Format: dataItem?._attribute = dict["_attribute"]    // My Custom Attribute
        
        // -----------------------------------------------------------------
        // EDIT CODE ABOVE ONLY
        // -----------------------------------------------------------------
        
        // Save new item
        mapper.save(dataItem!, completionHandler: {
            (error: Error?) -> Void in
            
            // If there was an error, print the error and return completion
            if let error = error {
                print("Error in CloudManager.uploadMeta(): \(error)")
                completion?(false); return
            }
            
            // If there was no error, print a notification that upload was successful
            print("Message from CloudManager.uploadMeta(): Metadata upload successful")
            completion?(true)
            
        })
        
    }
    
    // Define function for removing all user metadata from DynamoDB database
    public func removeAllUserMetadata(forUser user: String, completion: ((Bool) -> Void)? = nil) {
        
        // Query all items for selected user and delete each item //
        
        // Initialize object mapper
        let mapper = AWSDynamoDBObjectMapper.default()
        
        // Set filter settings
        let queryExpression = AWSDynamoDBQueryExpression()
        
        // Set condition for key
        queryExpression.keyConditionExpression = "#userId = :userID"
        // Map condition name to name in ProviderData database
        queryExpression.expressionAttributeNames = ["#userId": "userId"]
        // Map condition value to corresponding variable defined in this function
        queryExpression.expressionAttributeValues = [":userName": user]
        
        // Scan ProviderData with filter
        mapper.query(DatabaseClass.self, expression: queryExpression).continueWith(block: { (queryTask) -> Any? in
            
            // If there is an error, print an error message and return completion
            if let error = queryTask.error as NSError? {
                print("Error in CloudManager.removeAllUserMetadata(): \(error)"); completion?(false)
                
                // If the query was successful...
            } else if let dataItem = queryTask.result {
                // For each entry in the query...
                for entry in dataItem.items {
                    // Remove the entry
                    mapper.remove(entry).continueWith(block: { (removeTask) -> Any? in
                        // If there is an error, print an error message and return completion
                        if let error = removeTask.error as NSError? {
                            print("Error in CloudManager.removeAllUserMetadata(): \(error)")
                        }
                        return nil
                    })
                }
                
                // Return completion
                completion?(true)
            }
            return nil
        })
        
    }
    
}

// This is a generic class for uploading metadata to DynamoDB
private class DatabaseClass: AWSDynamoDBObjectModel, AWSDynamoDBModeling {
    
    // Required table attributes
    @objc public var _userID: String?
    @objc public var _eventDate: String?
    
    // -----------------------------------------------------------------
    // EDIT CODE BELOW ONLY
    // -----------------------------------------------------------------
    
    // MARK: EDIT
    // Specify additional (optional) attributes here
    // @objc public var _customMetadataAttribute: String? ...
    
    // Specify table name here
    class func dynamoDBTableName() -> String {
        return "TableName"
    }
    
    // Create a table of key - property values
    // If additional attributes were specified above, they should be added as a row to this table
    // Format: "_customMetadataAttribute : "customMetadataAttribute",
    override class func jsonKeyPathsByPropertyKey() -> [AnyHashable: Any] {
        return [
            "_userId" : "userId",
            "_eventDate" : "eventDate",
        ]
    }
    
    // -----------------------------------------------------------------
    // EDIT CODE ABOVE ONLY
    // -----------------------------------------------------------------
    
    // Hash key attribute (do not modify)
    class func hashKeyAttribute() -> String {
        return "_userID"
    }
    
    // Range key attribute (do not modify)
    class func rangeKeyAttribute() -> String {
        return "_eventDate"
    }
    
    
}
