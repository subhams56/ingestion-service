Steps to follow : 
1. Clone the repository using : git clone https://github.com/subhams56/ingestion-service.git 
   - cd into the ingestion-service folder (you can use the master branch)
2. Make sure Docker Desktop is running , then open terminal (gitbash) and run the command : docker compose up -d 
   - it will pull the kafka image and run the kafka container in the docker.
   - check the kafka running or not using the UI - localhost:8085.
   
3. Producer Script Steps :
   - Check if your system has python installed by command : python --version 
   - If python not installed , install python first.
   - Open the python scripts folder inside VS Code .
   - Run the telecom_producer script using command : python telecom_producer.py 
   - If everything done correctly you will see the script running and data getting produced in the console every 5 seconds.

4. Ingestion Microservice :
	- Once both Kafka in Docker and Producer script started , open the cloned microservice using Intellij IDE
	- Make sure your Database Docker container is also up and running.
	- Go to application.properties and edit the database properties according to your Database configs (url , username & Password )
	- Run the Ingestion microservice , if started successfully you will see "INGESTION SERVICE STARTUP STATUS" in the logs with all the details.

Full Architecture Flow with Ingestion-Service to Insights Service :
	<img width="1307" height="868" alt="Architecture flow" src="https://github.com/user-attachments/assets/0027bff9-1063-4c06-b177-2c033ac0ce2a" />
