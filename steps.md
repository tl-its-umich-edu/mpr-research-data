# mpr-research-data - - steps.md

1. Get production MPR DB credential file `database-research_ro.json` from:
   https://drive.google.com/file/d/1OnY1SLatPxLHN2iL2r1cidYEhVzMFIY-  
   > Using Google Drive web UI,  
   > `database-research_ro.json` file can be found in 
   > "SCW | M-Write" shared Google Drive under  
   > "MPR Technical" > "MPR Config Secrets" > "Prod".
2. Get GCP credentials for accessing bucket
3. Use `docker-compose up --build` to run the application  
   1. When running in local development environment, be sure to
      activate VPN so the application can reach the MPR DB.  The
      "VPN-remote-ITS_Special_Developer_Access" profile **_MUST_**
      be used.
   3. If `start.sh` ends with `sleep infinity` (or something similar) to
      keep the container running after the application exits, press ^C
      to stop the container when desired.
   4. To view the console output of the container, run:  
      ```
      docker-compose logs -t
      ```  
      to see it.  The log will remain available until the container is
      removed, e.g., using `docker-compose down`.
   5. While the container is running, interactive access to a shell
      within the container can be obtained using:  
      ```
      docker-compose exec job /bin/bash
      ```
