# mpr-research-data - - README.md

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
Note: If making a new Service Account, please make sure the Service Account has appropiate permissions to be able to access and modify the GCP bucket.

3. Use the `.env.sample` in the config folder to create an `.env` file in the root of the directory. 
   > Specify the settings for DB key using values from the `database-research_ro.json` file.
   > Paste the GCP JSON key into the `.env` file in `GCP_KEY`.
Note: You need to have these keys, there are no default values.

4. Adjust variables in the `.env` file before running if not applying defaults:
   > Specify the GCP Bucket info here. Defaults to: mpr-research-data-uploads.
      `GCLOUD_BUCKET`
   > Specify the number of months backwards to search courses. Defaults to 4:
      `NUMBER_OF_MONTHS` 

5. Use `docker-compose up --build` to run the application  
   1. When running in local development environment, be sure to
      activate VPN so the application can reach the MPR DB.  The
      "VPN-remote-ITS_Special_Developer_Access" profile **_MUST_** be used.
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
