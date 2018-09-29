Pod::Spec.new do |s|

  s.name         = "SwiftyForesight"
  s.version      = "1.0.0"
  s.summary      = "Foresight Swift API"
  s.description  = "Swift API for integrating mobile applications with Foresight cloud framework."
  s.homepage     = "https://github.com/Enabyl/SwiftyForesight.git"
  s.license      = "MIT"
  s.author       = { "Jonathan Zia" => "jzia@enabyl.me" }
  s.platform     = :ios, "12.0"
  s.source       = { :git => "https://github.com/Enabyl/SwiftyForesight.git", :tag => "1.0.0" }
  s.source_files = "SwiftyForesight/**/*.{h,m,swift}"
  s.framework    = "CoreML"
  s.dependency "AWSMobileClient"
  s.dependency "AWSDynamoDB"
  s.dependency "AWSCognito"
  s.dependency "AWSS3"

end
