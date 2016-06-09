# arl_verify
Chemical Transport Model Verification


This is a simple program right now that is still under construction.  

Right now I have written a simple functoin to download EPA AirNow data for a given set of dates.  

Example on how to use:

  <code>
  ipython 
  </code>
  
  <code>
  import airnow 
  </code>
  
  <code>
  an = airnow.airnow()
  </code>
  
  <code>
  print an.dates
  </code>
  
  #dates can be any array of python datetime objects
  
  ###The EPA AirNow ftp server requires a username and
  password to access.  Enter it
  
  <code>
  an.username = 'Enter your username here'
  </code>
  
  <code>
  an.password = 'Enter your password here'
  </code>
  #Retrieve the data into a pandas array
  an.retrieve_hourly_files()
  
  #data is now stored in the pandas array an.df
  
  <code>
  an.df.head()
  </code>
  
  
  #want to write it out to a file? go ahead
  
  <code>
  an.df.to_csv('output.txt')
  </code>
  
  #want a hdf file
  
  <code>
  an.df.to_hdf('output.hdf','df',format='table')
  </code>
  
  
