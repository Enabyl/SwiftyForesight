//
//  LibraData.swift
//  SwiftyForesight
//
//  Created by Jonathan Zia on 9/25/18.
//  Copyright © 2018 Enabyl Inc. All rights reserved.
//

import Foundation

// LibraData is a Swift class for formatting data gathered by the device
// for upload to remote servers for LibraServer processing.

public class LibraData {
    
    // MARK: Member Variables
    
    // The LibraData object requires the following variables to format
    // data files for upload. The .csv file will have the following format:
    // [time, inFeature1 ... inFeatureN, outFeature1 ... outFeatureM](t = 1)
    // ...
    // [time, inFeature1 ... inFeatureN, outFeature1 ... outFeatureM](t = T)
    public var timestamp: Bool          // Indicate whether data contains timestamp column
    public var numInputFeatures: Int    // Number of input features
    public var numOutputFeatures: Int   // Number of output features
    
    // Initialize private variables for Libra Error Logs filepaths
    private var errorLocalFilenames: URL        // URL of file holding filenames for local files unsuccessfully uploaded
    private var errorRemoteFilenames: URL       // Corresponding remote filenames for unsuccessful uploads
    
    // Define root filepath
    private var rootFilepath = try! FileManager.default.url(for: .documentDirectory, in: .userDomainMask, appropriateFor: nil, create: false)
    
    // Initialize placeholder for features and labels
    // Key : Value pairs include:
    // "Features" : [[Double]]
    // "Labels" : [[Double]]
    public var data = [String : [[Double]]]()
    
    // Initialize placeholder for metadata
    // Metadata is stored in a relational database. The sort key is always the user ID and the range key is always the upload date. These values are stored in a dictionary along with the other attributes, which are always stored as strings.
    // AttributeName: AttributeValue
    public var metadata = [String: String]()
    
    // Placeholder for CloudManager object associated with this LibraData object
    private var cloudManager: CloudManager
    
    // MARK: Initializer
    
    // Create a public initializer
    public init(hasTimestamps timestamp: Bool, featureVectors numInputs: Int, labelVectorLength numOutputs: Int, withManager manager: CloudManager) {
        
        // Initialize member variables with those declared in initializer
        self.timestamp = timestamp
        
        // If there is a timestamp column, count this as an input feature
        // Note that timestamp information is used by LibraServer but does not change local behavior
        self.numInputFeatures = (self.timestamp) ? (numInputs + 1) : (numInputs)
        self.numOutputFeatures = numOutputs
        
        // Each LibraData object will use a CloudManager object for managing data transfer
        self.cloudManager = manager
        
        // Initialize Libra Error Logs filepaths
        self.errorLocalFilenames = rootFilepath.appendingPathComponent("libraerrorlogs_local.plist")
        self.errorRemoteFilenames = rootFilepath.appendingPathComponent("libraerrorlogs_remote.plist")
        
    }
    
    // MARK: Data Population
    
    // Function for adding features to LibraData object
    // The data array should be formatted as follows:
    // [[F1] [F2] ... [Fn]] where Fn is the n'th feature vector
    // If there is a timestamp vector associated with this data, format it as follows:
    // [[T] [F1] [F2] ... [Fn]] where T is the timestep vector
    public func addFeatures(_ features: [[Double]]) {
        
        // Ensure that there is the right amount of feature vectors
        if features.count != self.numInputFeatures {
            // Print an error message and return
            print("Error in LibraData.addFeatures(): Incorrect number of feature vectors")
            return
        }
        
        // Ensure that feature vectors have the same length
        let properLength = features[0].count
        for vector in features {
            if vector.count != properLength {
                // Print an error message and return
                print("Error in LibraData.addFeatures(): Feature vectors are not the same length")
                return
            }
        }
        
        // Update values if no errors were encountered
        self.data.updateValue(features, forKey: LibraDataKeys.features.rawValue)
    }
    
    // Function for adding labels to LibraData object
    // The data array should be formatted as follows:
    // [L] = [[L1 L2 ... Lm](t=1), [L1 L2 ... Lm](t=2) ... [L1 L2 ... Lm](t=T)]
    // where numOutputFeatures = m and length(Fn) = T
    public func addLabels(_ labels: [[Double]]) {
        
        // Ensure that the label vectors have the right length
        for label in labels {
            if label.count != self.numOutputFeatures {
                // Print an error message and return
                print("Error in LibraData.addLabels(): Label vector length is incorrect")
                return
            }
        }
        
        // Update values if no errors were encountered
        self.data.updateValue(labels, forKey: LibraDataKeys.labels.rawValue)
    }
    
    // Function for adding metadata to LibraData object
    // Format: [AttributeName1:AttributeValue1, AttributeName2:AttributeValue2 ...]
    // NOTE: The first attribute is the sort key and the second attribute is the range key
    // NOTE: The first attribute key is always "_userID" and second attribute key is always "_eventDate"
    // NOTE: All subsequent keys must have the format "_m0" ... "_m9". Up to 10 additional metadata fields may be specified.
    // E.g. attributes = ["_userID":"johnsmith", "_eventDate":"07102018125556", "_m0": "65"]
    public func addMetadata(withAttributes attributes: [String: String]) {
        self.metadata = attributes
    }
    
    // MARK: Modifying Data
    
    // Function for clearing LibraData dictionaries
    public func clearData() {
        self.data.removeAll()
        self.metadata.removeAll()
    }
    
    // Function for formatting .csv file from LibraData object
    // The output will be a Data object saved to a specified URL with specified filename
    public func formatData(completion: ((Bool, Data, URL) -> Void)? = nil) {
        
        // Initialize formatter to format date (for filename)
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyyMMddHHmmss"
        
        // Set save path
        let savePath = self.rootFilepath.appendingPathComponent("\(cloudManager.userID)_\(formatter.string(from: Date())).csv")
        
        // Set placeholder for string
        var dataString: String = ""
        
        // The output Data object will have the following syntax:
        // t1, f1 ... fn, l1 ... ln\nt2, f1 ... fn, l1 ... ln
        // If there is no timestamp column, t1 = f1.
        
        // NOTE: Completion has two arguments:
        // Bool: Boolean indicating whether operation was successful
        // Data: Data object resulting from completed operation
        
        // ERROR CHECKING
        
        // First, ensure that the appropriate key-value pairs exist
        guard let _ = self.data[LibraDataKeys.features.rawValue] else {
            // Print error message and return completion
            print("Error in LibraData.formatData(): Features have not been written to LibraData.data")
            completion?(false, Data(), savePath); return}
        guard let _ = self.data[LibraDataKeys.labels.rawValue] else {
            // Print error message and return completion
            print("Error in LibraData.formatData(): Labels have not been written to LibraData.data")
            completion?(false, Data(), savePath); return}
        
        // Ensure that the number of feature vectors is correct
        if self.data[LibraDataKeys.features.rawValue]!.count != self.numInputFeatures {
            // Print an error message and return completion
            print("Error in LibraData.formatData(): The number of feature vectors is not correct")
            completion?(false, Data(), savePath); return
        }
        
        // Ensure that all vectors in features and labels have the same length
        // Every vector should have the same length as the first feature vector
        let properLength = self.data[LibraDataKeys.features.rawValue]![0].count
        // Loop through all feature vectors and ensure that this condition is true
        for vector in self.data[LibraDataKeys.features.rawValue]! {
            if vector.count != properLength {
                // Print an error message and return completion
                print("Error in LibraData.formatData(): Feature vectors do not have the same length")
                completion?(false, Data(), savePath); return
            }
        }
        
        // Ensure that there is a proper number of label vectors
        if self.data[LibraDataKeys.labels.rawValue]!.count != properLength {
            // Print an error message and return completion
            print("Error in LibraData.formatData(): The number of label vectors and features is not the same")
            completion?(false, Data(), savePath); return
        }
        
        // Ensure that each label vector has the same length
        for vector in self.data[LibraDataKeys.labels.rawValue]! {
            if vector.count != self.numOutputFeatures {
                // Print an error message and return completion
                print("Error in LibraData.formatData(): The label vectors do not have the proper length")
                completion?(false, Data(), savePath); return
            }
        }
        
        // FORMATTING DATA
        
        // For each element, format the output appropriately
        for i in 0..<self.data["Features"]![0].count {
            
            // Write input features to string
            for j in 0..<self.numInputFeatures {
                dataString += "\(self.data[LibraDataKeys.features.rawValue]![j][i]),"
            }
            
            // Write output features (labels) to string
            for j in 0..<self.numOutputFeatures {
                dataString += "\(self.data[LibraDataKeys.labels.rawValue]![i][j])"
                
                // Append comma for all entries but the last one
                if j != self.numOutputFeatures-1 {
                    dataString += ","
                }
            }
            
            // Write newline to string
            dataString += "\n"
            
        }
        
        // WRITING DATA
        
        // Write string to file
        do {
            try dataString.write(to: savePath, atomically: true, encoding: String.Encoding.utf8)
        } catch {
            // Print error message if the write operation failed
            print("Error in LibraData.formatData(): Unable to write to file")
            completion?(false, Data(), savePath); return
        }
        
        // Write contents of URL to data object for subsequent use
        if let dataObject = try? Data(contentsOf: savePath) {
            completion?(true, dataObject, savePath); return
        } else {
            // If there is an error, print an error message and return completion
            print("Error in LibraData.formatData(): Unable to read from file")
            completion?(false, Data(), savePath); return
        }
        
    }
    
    // MARK: File Management
    
    // Function for removing .csv file stored locally (when no longer needed)
    public func removeLocalFile(atPath filePath: URL, completion: ((Bool) -> Void)? = nil) {
        
        // Ensure a file exists at the local filepath
        guard FileManager.default.fileExists(atPath: filePath.path) else {
            // Print error message and return completion
            print("Error in LibraData.removeLocalFile(): File not found at localFilepath")
            completion?(false)
            return
        }
        
        // Remove file at specified URL
        do {
            try FileManager.default.removeItem(at: filePath)
            completion?(true)
        } catch {
            // Print error message and return completion
            print("Error in LibraData.removeLocalFile(): Unable to remove file")
            completion?(false)
        }
        
    }
    
    // Define function for clearing data from Libra Error Logs
    public func clearErrorLogs(completion: ((Bool) -> Void)? = nil) {
        
        // Ensure that necessary files exist
        guard FileManager.default.fileExists(atPath: self.errorLocalFilenames.path) && FileManager.default.fileExists(atPath: self.errorRemoteFilenames.path) else {
            // Print error message and return
            print("Error in LibraData.clearErrorLogs(): Log files do not exist")
            completion?(false); return
        }
        
        // Clear local .csv files
        // Get filepaths from error logs
        let localFiles = NSArray(contentsOf: self.errorLocalFilenames) as! [String]
        for file in localFiles {
            // Remove the .csv file located at the filepath
            self.removeLocalFile(atPath: self.rootFilepath.appendingPathComponent(file))
        }
        
        // Overwrite log files
        let localArray = [String]()      // Temporary empty array of local filenames
        let remoteArray = [String]()     // Temporary empty array of remote filenames
        (localArray as NSArray).write(to: self.errorLocalFilenames, atomically: true)
        (remoteArray as NSArray).write(to: self.errorRemoteFilenames, atomically: true)
        // Return completion
        completion?(true)
        
    }
    
    // MARK: Upload/Download
    
    // Function for uploading metadata to remote database
    public func uploadMetadataToRemote(completion: ((Bool) -> Void)? = nil) {
        
        // Ensure that data has been written to self.metadata
        guard !self.metadata.isEmpty else {
            // Print error message and return completion
            print("Error in LibraData.uploadMetaDataToRemote(): No data written to LibraData.metadata")
            completion?(false); return
        }
        
        // Ensure that dictionary contains values for hash and range keys
        // Hash Key
        guard self.metadata[Keys.hash] != nil else {
            // Print error and return completion
            print("Error in LibraData.uploadMetaDataToRemote(): No value for _userID (Keys.hash) attribute")
            completion?(false); return
        }
        
        // Range Key
        guard self.metadata[Keys.range] != nil else {
            // Print error and return completion
            print("Error in LibraData.uploadMetaDataToRemote(): No value for _eventDate (Keys.range) attribute")
            completion?(false); return
        }
        
        self.cloudManager.uploadMetadata(forDictionary: self.metadata) { (success) in
            // If successful, print notification and return completion
            if success {
                print("Message from LibraData.uploadMetadataToRemote: Upload successful"); completion?(true)
            } else {
                // If not successful, return completion
                completion?(false)
            }
        }
    }
    
    // Function for querying metadata from remote database (returns DatabaseClass object)
    public func queryMetadataFromRemote(fromDate startDate: Date, toDate endDate: Date, completion: ((Bool, [DatabaseClass]) -> Void)? = nil) {
        
        // Ensure that the begin date precedes the end date
        guard startDate <= endDate else {
            // Print error and return completion
            print("Error in LibraData.queryMetadataFromRemote(): beginDate must precede endDate")
            completion?(false, [DatabaseClass]()); return
        }
        
        self.cloudManager.queryMetadata(fromDate: startDate, toDate: endDate) { (success, data) in
            // If successful, print notification and return completion
            if success {
                print("Message from LibraData.queryMetadataFromRemote(): Query successful"); completion?(true, data)
            } else {
                // If not successful, return completion
                completion?(false, [DatabaseClass]())
            }
        }
    }
    
    // Function for uploading data to remote server
    public func uploadDataToRemote(fromLocalPath localPath: URL, completion: ((Bool) -> Void)? = nil) {
        
        // Set remote filename
        let remoteName = localPath.lastPathComponent
        
        // Ensure a file exists at the localFilepath
        guard FileManager.default.fileExists(atPath: localPath.path) else {
            // Print error message and return completion
            print("Error in LibraData.uploadDataToRemote(): File not found at localFilepath")
            completion?(false)
            return
        }
        
        // Upload data using Cloud Manager
        cloudManager.uploadFile(Local: localPath, Remote: remoteName) { (success) in
            
            // UPLOAD ERROR HANDLING
            
            // When an upload fails, append the filename to a local file in the documents directory for future upload.
            // 1. Check if file exists at Documents/libraerrorlogs.txt
            // 2. If not, create the file and write "filepath,\n"
            // 3. If so, append the filename to the end of the file
            
            guard !success else {
                
                // If successful, remove local .csv file
                self.removeLocalFile(atPath: localPath)
                
                // Return completion
                completion?(true); return
                
            }
            
            // Print error message
            print("Message from LibraData.uploadDataToRemote(): Retry upload at a later time with LibraData.retryFailedUploads()")
            
            // Attempt to obtain a list of filepaths from libraerrorlogs.txt
            if FileManager.default.fileExists(atPath: self.errorLocalFilenames.path) && FileManager.default.fileExists(atPath: self.errorRemoteFilenames.path) {
                // Get local and remote filenames from the file
                var local = NSArray(contentsOf: self.errorLocalFilenames) as! [String]
                var remote = NSArray(contentsOf: self.errorRemoteFilenames) as! [String]
                
                // Ensure that both arrays have the same length. Else, logfiles are corrupted.
                guard local.count == remote.count else {
                    // Print error, clear logs, and return
                    print("Error in LibraData.uploadDataToRemote(): Libra Error Logs corrupted. Clearing cache.")
                    self.clearErrorLogs()                   // Clear error logs
                    self.removeLocalFile(atPath: localPath) // Remove most recent file (not added to logs yet)
                    return
                }
                
                // Append current filenames to list
                local.append(localPath.lastPathComponent)
                remote.append(remoteName)
                // Save arrays to error logs
                (local as NSArray).write(to: self.errorLocalFilenames, atomically: true)
                (remote as NSArray).write(to: self.errorRemoteFilenames, atomically: true)
                
            } else {
                
                // If there is no file at the location yet, create lists with the current filenames and save
                let local = [localPath.lastPathComponent] as NSArray
                let remote = [remoteName] as NSArray
                local.write(to: self.errorLocalFilenames, atomically: true)
                remote.write(to: self.errorRemoteFilenames, atomically: true)
                
            }
            
            // Return completion
            completion?(false)
            
        }
        
    }
    
    // Function for uploading files from a list of filenames
    // This is intended as error handling in the case where a poor network connection prevented files from being uploaded.
    public func retryFailedUploads(completion: ((Bool) -> Void)? = nil) {
        
        // 1. Check for error log file existence
        // 2. If not, return an error
        // 3. If so, loop through each name in the list and attempt to upload file at specified path
        // 4. Remove entry whenever successful
        
        // 1. Check for file existence
        guard FileManager.default.fileExists(atPath: self.errorLocalFilenames.path) && FileManager.default.fileExists(atPath: self.errorRemoteFilenames.path) else {
            // 2. Print error message and return completion
            print("Error in LibraData.retryFailedUploads(): Libra Error Logs not found")
            completion?(false); return
        }
        
        // Get filenames and filepaths from URLs
        let local = NSArray(contentsOf: self.errorLocalFilenames) as! [String]
        let remote = NSArray(contentsOf: self.errorRemoteFilenames) as! [String]
        
        // Ensure that the arrays have the same number of elements; else, they are corrupted.
        guard local.count == remote.count else {
            // Print error, clear logs, and return
            print("Error in LibraData.retryFailedUploads(): Libra Error Logs corrupted. Clearing cache.")
            self.clearErrorLogs()
            completion?(false); return
        }
        
        // Ensure that the arrays have more than 0 elements
        guard local.count > 0 else {
            // Print error and return
            print("Error in LibraData.retryFailedUploads(): No data to retry uploading")
            completion?(false); return
        }
        
        // Define return value placeholders
        var updatedLocal = [String]()
        var updatedRemote = [String]()
        
        // 4. Loop through filenames and attempt to upload file
        for i in 0..<local.count {
            // Attempt file upload
            cloudManager.uploadFile(Local: self.rootFilepath.appendingPathComponent(local[i]), Remote: remote[i]) { (success) in
                // 4. If unsuccessful, append index to return value array
                if !success {
                    updatedLocal.append(local[i])
                    updatedRemote.append(remote[i])
                } else {
                    // Else, if successful, remove local .csv file
                    self.removeLocalFile(atPath: self.rootFilepath.appendingPathComponent(local[i]))
                }
            }
            
        }
        
        // Write updated filepaths and filenames back to Libra Error Logs
        (updatedLocal as NSArray).write(to: self.errorLocalFilenames, atomically: true)
        (updatedRemote as NSArray).write(to: self.errorRemoteFilenames, atomically: true)
        
        // Return completion (asynchronous)
        completion?(true)
        
    }
    
}

// Enum for defining keys in LibraData.data dictionary
private enum LibraDataKeys: String {
    case features = "Features"
    case labels = "Labels"
}
