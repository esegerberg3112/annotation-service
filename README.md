# GAS Annotation Service  
Capstone project. Fully functional SaaS genomics annotator that uses the AnnTools package. User can submit files for annotation, and then view and download results. There are free and premium user tiers, with premium users having permanent access to their results while free users have their results files archived to AWS Glacier after a short period of time.

Overall Architecture:  

<img width="750" alt="Screenshot 2024-03-14 at 1 12 42 PM" src="https://github.com/esegerberg3112/annotation-service/assets/61920056/33bdf237-2c94-40b1-9003-21a260cfaaab">

AWS Cloud Features Used:  
1. ELB / ASG - leveraged launch templates and ELB's to setup dynamic auto-scaling for the web and annotator services
2. EC2 - for hosting servers, with 3 separate types for the web instance, annotator, and utility services
3. S3 - for storage of user uploaded files and annotation results
4. DynamoDB - storing annotation job information
5. SNS / SQS - for async, interservice communication
6. Lambda - for email notifications to users when jobs were finished
7. S3 Glacier - for archiving results files, and restoring when requested, for free users

Technology Stack:

HTML/JS/CSS
Python
PostgreSQL
DynamoDB
