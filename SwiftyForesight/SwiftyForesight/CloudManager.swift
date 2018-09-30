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
    
    // Initialize static variable for holding DynamoDB table name
    public static var tableName = String()
    
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
    public func removeAllUserFiles(completion: ((Bool) -> Void)? = nil) {
        
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
        listRequest?.prefix = self.userID
        
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
        
        // Create DynamoDB Object Mapper
        let mapper = AWSDynamoDBObjectMapper.default()
        
        // Create a data item for organizing data for upload
        let dataItem = DatabaseClass()
        
        // Write required metadata items to DatabaseClass
        dataItem?._userID = dict[Keys.hash]     // Hash Key (userID)
        dataItem?._eventDate = dict[Keys.range] // Range Key (eventDate)
        
        // Add additional metadata fields if provided by the user
        if dict[Keys.m0] != nil { dataItem?._m0 = dict[Keys.m0] }   // M0
        if dict[Keys.m1] != nil { dataItem?._m1 = dict[Keys.m1] }   // M1
        if dict[Keys.m2] != nil { dataItem?._m2 = dict[Keys.m2] }   // M2
        if dict[Keys.m3] != nil { dataItem?._m3 = dict[Keys.m3] }   // M3
        if dict[Keys.m4] != nil { dataItem?._m4 = dict[Keys.m4] }   // M4
        if dict[Keys.m5] != nil { dataItem?._m5 = dict[Keys.m5] }   // M5
        if dict[Keys.m6] != nil { dataItem?._m6 = dict[Keys.m6] }   // M6
        if dict[Keys.m7] != nil { dataItem?._m7 = dict[Keys.m7] }   // M7
        if dict[Keys.m8] != nil { dataItem?._m8 = dict[Keys.m8] }   // M8
        if dict[Keys.m9] != nil { dataItem?._m9 = dict[Keys.m9] }   // M9
        
        // Save new item
        mapper.save(dataItem!, completionHandler: {
            (error: Error?) -> Void in
            
            // If there was an error, print the error and return completion
            if let error = error {
                print("Error in CloudManager.uploadMetadata(): \(error)")
                completion?(false); return
            }
            
            // If there was no error, print a notification that upload was successful
            print("Message from CloudManager.uploadMetadata(): Metadata upload successful")
            completion?(true)
            
        })
        
    }
    
    // Define function for querying metadata between two dates
    public func queryMetadata(fromDate beginDate: Date, toDate endDate: Date, completion: ((Bool, [DatabaseClass]) -> Void)? = nil) {
        
        // Initialize placeholder return items
        var returnItem = [DatabaseClass]()
        
        // Convert startDate and stopDate to strings
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyyMMddHHmmss"
        let fromDate = formatter.string(from: beginDate)
        let toDate = formatter.string(from: endDate)
        
        // Initialize object mapper
        let mapper = AWSDynamoDBObjectMapper.default()
        
        // Set filter settings
        let queryExpression = AWSDynamoDBQueryExpression()
        // Set conditions for keys
        queryExpression.keyConditionExpression = "#userID = :userID AND #eventDate BETWEEN :fromDate AND :toDate"
        // Map condition names to names in ProviderData database
        queryExpression.expressionAttributeNames = ["#userID": "userID", "#eventDate": "eventDate"]
        // Map condition values to corresponding variables defined in this function
        queryExpression.expressionAttributeValues = [":userID": self.userID, ":fromDate": fromDate, ":toDate": toDate]
        
        // Scan ProviderData with filter
        mapper.query(DatabaseClass.self, expression: queryExpression).continueWith(block: { (task) -> Any? in
            
            // If there is an error, print a message and return completion
            if let error = task.error as NSError? {
                print("Error in CloudManager.queryMetadata(): \(error)"); completion?(false, returnItem)
                
                // If the query was successful...
            } else if let dataItem = task.result {
                if dataItem.items.count > 0 {
                    
                    // For each entry in the query...
                    for entry in dataItem.items as! [DatabaseClass] {
                        // Append entries to returnItem
                        returnItem.append(entry)
                    }
                }
                // Return completion
                completion?(true, returnItem)
            }
            return nil
        })
        
    }
    
    // Define function for removing all user metadata from DynamoDB database
    public func removeAllUserMetadata(completion: ((Bool) -> Void)? = nil) {
        
        // Query all items for selected user and delete each item //
        
        // Initialize object mapper
        let mapper = AWSDynamoDBObjectMapper.default()
        
        // Set filter settings
        let queryExpression = AWSDynamoDBQueryExpression()
        
        // Set condition for key
        queryExpression.keyConditionExpression = "#userID = :userID"
        // Map condition name to name in ProviderData database
        queryExpression.expressionAttributeNames = ["#userID": "userID"]
        // Map condition value to corresponding variable defined in this function
        queryExpression.expressionAttributeValues = [":userName": self.userID]
        
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
public class DatabaseClass: AWSDynamoDBObjectModel, AWSDynamoDBModeling {
    
    // Required table attributes
    @objc public var _userID: String?
    @objc public var _eventDate: String?
    
    // Optional additional metadata fields
    // The user should keep track of what data is being stored in each field
    @objc public var _m0: String?
    @objc public var _m1: String?
    @objc public var _m2: String?
    @objc public var _m3: String?
    @objc public var _m4: String?
    @objc public var _m5: String?
    @objc public var _m6: String?
    @objc public var _m7: String?
    @objc public var _m8: String?
    @objc public var _m9: String?
    
    // Specify table name here
    public class func dynamoDBTableName() -> String {
        return CloudManager.tableName
    }
    
    // Create a table of key - property values
    override public class func jsonKeyPathsByPropertyKey() -> [AnyHashable: Any] {
        return [
            Keys.hash : "userID",
            Keys.range : "eventDate",
            Keys.m0 : "m0",
            Keys.m1 : "m1",
            Keys.m2 : "m2",
            Keys.m3 : "m3",
            Keys.m4 : "m4",
            Keys.m5 : "m5",
            Keys.m6 : "m6",
            Keys.m7 : "m7",
            Keys.m8 : "m8",
            Keys.m9 : "m9",
        ]
    }
    
    // Hash key attribute (do not modify)
    public class func hashKeyAttribute() -> String {
        return "_userID"
    }
    
    // Range key attribute (do not modify)
    public class func rangeKeyAttribute() -> String {
        return "_eventDate"
    }
    
}

// Class holding database key names (preventing formatting errors)
public class Keys {
    static let hash = "_userID"
    static let range = "_eventDate"
    static let m0 = "_m0"
    static let m1 = "_m1"
    static let m2 = "_m2"
    static let m3 = "_m3"
    static let m4 = "_m4"
    static let m5 = "_m5"
    static let m6 = "_m6"
    static let m7 = "_m7"
    static let m8 = "_m8"
    static let m9 = "_m9"
}
