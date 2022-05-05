# mpr-research-data - - steps.md

1. Get production MPR DB credential file `database-research_ro.json` from:
   https://drive.google.com/file/d/1OnY1SLatPxLHN2iL2r1cidYEhVzMFIY-  
   > Using Google Drive web UI,  
   > `database-research_ro.json` file can be found in 
   > "SCW | M-Write" shared Google Drive under  
   > "MPR Technical" > "MPR Config Secrets" > "Prod".

2. Get GCP credentials for accessing bucket:
   Use Service Account 'dbToGCPHelper' for accessing and pushing to GCP.
   > Using GCP Web UI,
   > Go to "IAM & Admin" > "Service Accounts"
   > Use Service Account with name "dbTOGCPHelper"
   > Under "Actions", generate a new key-pair as a JSON file.
   > Save JSON as `gcpAccessKey.json`
Note: If making a new Service Account, please make sure the Service Account has appropiate permissions to be able to access and modify the GCP bucket.

3. Save the two credential JSON files made above to a folder called `Credentials` in the root of the project directory.
   Note: 
   > If folder is not created and script is executed, a blank folder called `Credentials` in the root will be made to put the keys in and the script will exit.
   > You can rename the JSON files but you will have to update the defaults in the `config.env`, in the root of the project directory.

4. Adjust variables in `config.env` before running if not applying defaults:
   > Specify the GCP Project and Bucket info in
      `GCLOUD_PROJECT` and `GCLOUD_BUCKET`
   > Specify the number of months backwards to search courses. Defaults to 4:
      `NUMBER_OF_MONTHS` 
   > Other variables such as default file names are described in `config.env`. 
   > Ideally, only two 2 Credential JSON files **_MUST_** be added, and the three config variables adjusted to run the scripts.

5. Use `docker-compose up --build` to run the application  
   1. When running in local development environment, be sure to
      activate VPN so the application can reach the MPR DB.  The
      "VPN-remote-ITS_Special_Developer_Access" profile **_MUST_**
      be used.
   2. If `start.sh` ends with `sleep infinity` (or something similar) to
      keep the container running after the application exits, press ^C
      to stop the container when desired.
      Note: `sleep infinity` is currently not active.
   3. To view the console output of the container, run:  
      ```
      docker-compose logs -t
      ```  
      to see it.  The log will remain available until the container is
      removed, e.g., using `docker-compose down`.
   4. While the container is running, interactive access to a shell
      within the container can be obtained using:  
      ```
      docker-compose exec job /bin/bash
      ```
