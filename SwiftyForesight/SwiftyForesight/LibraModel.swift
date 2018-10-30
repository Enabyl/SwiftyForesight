//
//  LibraModel.swift
//  SwiftyForesight
//
//  Created by Jonathan Zia on 9/25/18.
//  Copyright © 2018 Enabyl Inc. All rights reserved.
//

import Foundation
import CoreML

// This file contains the LibraModel class. This class manages CoreML model
// download, compilation, upload, and prediction.
// MARK: LibraModel Superclass
public class LibraModel {
    
    // Initialize attributes
    public var numFeatures: Int         // Number of input features (i.e. number of input feature vectors)
    public var localFilepath: URL       // Filepath for local storage of model
    public var compiled = false         // Flag indicating whether a compiled model is available
    
    // Initialize attribute placeholder for CoreML model
    public var model: MLModel
    
    // Set placeholder for model's CloudManager object
    private var cloudManager: CloudManager
    
    // Define initializer
    public init(modelClass model: MLModel, numInputFeatures features: Int, localFilepath filepath: URL, withManager manager: CloudManager) {
        
        // Ensure that there is more than one input feature
        if features < 1 {
            // Print notification and set numFeatures to 1
            print("Warning in LibraModel.init(): numInputFeatures invalid, setting to 1")
            self.numFeatures = 1
        } else {
            self.numFeatures = features
        }
        
        // Update attribute values
        self.cloudManager = manager
        self.localFilepath = filepath
        self.model = model
        
    }
    
    // Function for downloading and compiling model from server
    public func fetchFromRemote(withRemoteFilename filename: String, completion: ((Bool) -> Void)? = nil) {
        
        // Steps for fetching model include:
        // 1. Download model file to specified destination using CloudManager
        // 2. Try to compile the model. If successful, update model attribute
        
        // Remove the old model file if it is already present at the local filepath
        if FileManager.default.fileExists(atPath: self.localFilepath.path) {
            do {
                try FileManager.default.removeItem(at: self.localFilepath)
            } catch {
                // Print error message (for debugging)
                print("Error in LibraModel.fetchFromRemote(): Unable to overwrite existing model")
            }
        }
        
        // Download model file using CloudManager
        cloudManager.downloadFile(Remote: filename, Local: self.localFilepath) { (success) in
            
            // Continue if the download was successful
            guard success else {completion?(false); return}
            
            // Compile the model located in the specified URL
            do {
                
                // Get URL of compiled model
                let compiledURL = try MLModel.compileModel(at: self.localFilepath)
                // Print notification of success (for debugging)
                print("Message from LibraModel.fetchFromRemote(): Model compiled successfully")
                
                // Generate MLModel object from file at compiled URL
                let modelObject = try MLModel(contentsOf: compiledURL)
                // Print notification of success (for debugging)
                print("Message from LibraModel.fetchFromRemote(): Model generated successfully -- using new model")
                
                // If successful, overwrite LibraData.model attribute
                self.model = modelObject
                
                // Update compiled flag
                self.compiled = true
                
            } catch {
                // Print error
                print("Error in LibraModel.fetchFromRemote(): Unable to compile model after download")
                completion?(false)
            }
            
            // In either case, delete model file when complete to allow for subsequent overwrite
            do {
                try FileManager.default.removeItem(at: self.localFilepath)
                completion?(true)
            } catch {
                // Print error
                print("Error in LibraModel.fetchFromRemote(): Unable to remove local model file. Subsequent downloads may be unsuccessful unless a new local filepath is set.")
                completion?(false)
            }
            
        }
        
    }
    
}

// The following are subclasses for the different MLModel types, inheriting from LibraModel
// The two types of machine learning models currently supported fall into distinct categories:
// (1) Sequential models, e.g. LSTMs
// (2) Feedforward models, e.g. Softmax
// Each of these categories has its own feature provider and class handling prediction generation, as each model category has a different data architecture.

// MARK: Model Classes
// NOTE: These classes inherit from LibraModel
// Sequential Models class
public class SequentialModel: LibraModel {
    
    // Define a placeholder for the input feature names of this model
    // Note that the default names are input1, lstm_1_h_in, lstm_1_c_in, output1
    private var featureNames = ["input1", "lstm_1_h_in", "lstm_1_c_in", "output1"]
    
    // Function for setting custom feature names in model
    // For LSTM model, default names are: input1, lstm_1_h_in, lstm_1_c_in, output1
    // The names provided should correspond to the input, hidden state, and output state respectively
    public func setFeatureNames(to names: [String]) {
        
        // Verify that there are three names provided
        guard names.count == self.featureNames.count else {
            // Print error and return
            print("Error in SequentialModel.setFeatureNames(): Name vector should contain 3 elements\n")
            print("Proper Order: [input, hidden_state, output_state]")
            return
        }
        
        // Set feature names
        self.featureNames = names
        
    }
    
    // Function for generating predictions with sequential model
    public func predict(forInputs input: [[Double]], availableGPU gpu: Bool) -> MLMultiArray {
        
        // CHECK FOR ERRORS
        
        // Initialize empty array for return value
        var output = MLMultiArray()
        
        // First, ensure that the number of input vectors is correct
        guard input.count == self.numFeatures else {
            // Print error and return nil
            print("Error in SequentialModel.predict(): Ensure that there are \(self.numFeatures) input feature vectors")
            return output
        }
        
        // Second, ensure that each feature vector has the same nonzero length
        let properLength = input[0].count
        for vector in input {
            if vector.count != properLength {
                // Print error and return
                print("Error in SequentialModel.predict(): Input feature vectors do not have the same length")
                return output
            }
        }
        
        // GENERATE PREDICTIONS
        
        // The procedure for generating predictions with a sequential model is as follows:
        // 1. Check whether compiled model is available
        // 2. Generate input vector with proper formatting
        // 3. Check whether predictions should be made with CPU or GPU
        // 4. Use SequentialModelFeatureProvider to generate predictions from compiled model
        
        // 1. Check whether a compiled model is available
        guard self.compiled else {
            // Print error and return
            print("Error in SequentialModel.predict(): No compiled model on device")
            return output
        }
        
        // 2. Create placeholders for input and output data
        let inputVector = try? MLMultiArray(shape: [NSNumber(value: self.numFeatures)], dataType: MLMultiArrayDataType.double)
        var outputVector: MLFeatureProvider
        
        // Populate placeholder with input vectors
        for i in 0..<self.numFeatures { inputVector![i] = NSNumber(value: input[i][0]) }
        
        // Construct model input with MLFeatureProvider protocol
        var modelInput = SequentialModelFeatureProvider(input: inputVector!, features: Array(self.featureNames.dropLast()))
        
        // 3. Set prediction options based on GPU availability
        let options = MLPredictionOptions()         // Instantiate prediction options object
        options.usesCPUOnly = gpu ? false : true    // If the GPU is not available, set usesCPUOnly flag to true
        
        // 4. Obtain model output for the first timestep
        do {
            outputVector = try self.model.prediction(from: modelInput, options: options)
        } catch {
            // Print error and return
            print("Error in SequentialModel.predict(): Unable to generate prediction from model at step 1")
            return output
        }
        
        // For each subsequent timesetp of the sequential model, perform the following
        for i in 1..<properLength {
            
            // Generate input vector
            for j in 0..<self.numFeatures { inputVector![j] = NSNumber(value: input[j][i]) }
            // Construct model input with MLFeatureProvider protocol
            modelInput = SequentialModelFeatureProvider(input: inputVector!, features: Array(self.featureNames.dropLast()), hidden: outputVector.featureValue(for: self.featureNames[1])?.multiArrayValue, output: outputVector.featureValue(for: self.featureNames[2])?.multiArrayValue)
            
            // Obtain model output
            do {
                outputVector = try self.model.prediction(from: modelInput, options: options)
            } catch {
                // Print error and return
                print("Error in SequentialModel.predict(): Unable to generate prediction from model at step \(i)")
                return output
            }
            
        }
        
        // Return features at the final timestep
        output = outputVector.featureValue(for: self.featureNames[3])?.multiArrayValue ?? output
        return output
        
    }
    
}

// Feedforward Models class
public class FeedforwardModel: LibraModel {
    
    // Define a placeholder for the input feature names of this model
    // Note that the default names are input1, output1
    private var featureNames = ["input1", "output1"]
    
    // Function for setting custom feature names in model
    // For LSTM model, default names are: input1, output1
    // The names provided should correspond to the input and output vectors respectively
    public func setFeatureNames(to names: [String]) {
        
        // Verify that there are three names provided
        guard names.count == self.featureNames.count else {
            // Print error and return
            print("Error in FeedforwardModel.setFeatureNames(): Name vector should contain 3 elements\n")
            print("Proper Order: [input, output]")
            return
        }
        
        // Set feature names
        self.featureNames = names
        
    }
    
    // Function for generating predictions with feedforward model
    public func predict(forInputs input: [Double], availableGPU gpu: Bool) -> MLMultiArray {
        
        // CHECK FOR ERRORS
        
        // Initialize empty array for return value
        var output = MLMultiArray()
        
        // First, ensure that the input vector length is correct
        guard input.count == self.numFeatures else {
            // Print error and return nil
            print("Error in FeedforwardModel.predict(): Ensure that there are \(self.numFeatures) input feature vectors")
            return output
        }
        
        // GENERATE PREDICTIONS
        
        // The procedure for generating predictions with a feedforward model is as follows:
        // 1. Check whether compiled model is available
        // 2. Generate input vector with proper formatting
        // 3. Check whether predictions should be made with CPU or GPU
        // 4. Use FeedforwardModelFeatureProvider to generate predictions from compiled model
        
        // 1. Check whether a compiled model is available
        guard self.compiled else {
            // Print error and return
            print("Error in SequentialModel.predict(): No compiled model on device")
            return output
        }
        
        // 2. Create placeholders for input and output data
        let inputVector = try? MLMultiArray(shape: [NSNumber(value: self.numFeatures)], dataType: MLMultiArrayDataType.double)
        var outputVector: MLFeatureProvider
        
        // Populate placeholder with input features
        for i in 0..<self.numFeatures { inputVector![i] = NSNumber(value: input[i]) }
        
        // Construct model input with MLFeatureProvider protocol
        let modelInput = FeedforwardModelFeatureProvider(input: inputVector!, features: Array(self.featureNames.dropLast()))
        
        // 3. Set prediction options based on GPU availability
        let options = MLPredictionOptions()         // Instantiate prediction options object
        options.usesCPUOnly = gpu ? false : true    // If the GPU is not available, set usesCPUOnly flag to true
        
        // 4. Obtain model output
        do {
            outputVector = try self.model.prediction(from: modelInput, options: options)
        } catch {
            // Print error and return
            print("Error in FeedforwardModel.predict(): Unable to generate prediction from model")
            return output
        }
        
        // Return output vector
        output = outputVector.featureValue(for: self.featureNames[1])?.multiArrayValue ?? output
        return output
        
    }
    
}


// MARK: Feature Providers
// Sequential Models feature provider
private class SequentialModelFeatureProvider: MLFeatureProvider {
    
    // Initialize model placeholders
    var input: MLMultiArray         // Input vector
    var hidden: MLMultiArray? = nil // Hidden state
    var output: MLMultiArray? = nil // Output state
    
    // Set placeholder for feature names (enabling customization)
    var features: [String]
    
    // Set computed value for returning feature names
    public var featureNames: Set<String> {
        get { return Set<String>(self.features) }
    }
    
    // Return feature values from feature names
    public func featureValue(for featureName: String) -> MLFeatureValue? {
        // Return feature based on feature name
        switch featureName {
        case self.features[0]:  // Input feature
            return MLFeatureValue(multiArray: input)
        case self.features[1]:  // Hidden state
            return (hidden == nil) ? nil : MLFeatureValue(multiArray: hidden!)
        case self.features[2]:  // Output state
            return (output == nil) ? nil : MLFeatureValue(multiArray: output!)
        default:
            // Print error and return nil
            print("Error in SequentialModelFeatureProvider.featureValue(): Invalid feature name")
            return nil
        }
    }
    
    public init(input: MLMultiArray, features: [String], hidden: MLMultiArray? = nil, output: MLMultiArray? = nil) {
        // Set attribute values
        self.input = input
        self.features = features
        self.hidden = hidden
        self.output = output
    }
    
}

// Feedforward Models feature provider
private class FeedforwardModelFeatureProvider: MLFeatureProvider {
    
    // Initialize model placeholders
    var input: MLMultiArray     // Input Vector
    
    // Set placeholder for feature names (enabling customization)
    var features: [String]
    
    // Set computed value for returning feature names
    public var featureNames: Set<String> {
        get { return Set<String>(features) }
    }
    
    public func featureValue(for featureName: String) -> MLFeatureValue? {
        // Return feature based on feature name
        switch featureName {
        case self.features[0]:  // Input features
            return MLFeatureValue(multiArray: input)
        default:
            // Print error and return nil
            print("Error in FeedforwardModelFeatureProvider.featureValue(): Inavlid feature name")
            return nil
        }
    }
    
    public init(input: MLMultiArray, features: [String]) {
        // Set attribute values
        self.input = input
        self.features = features
    }
    
}
