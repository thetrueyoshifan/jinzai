# Mugino MIITS Client for Sequenzia IntelliDex+
<img width="700" src="https://user-images.githubusercontent.com/15165770/211075998-eef8e302-768c-4a78-8de3-fc287bade11d.png"><br/>
Mugino MIITS Project, Deep Image Tagging System API Client for Sequenzia and IntelliDex

Simple interface for DeepBooru to process images from Sequenzia and retrieve tagging data async from normal operations

## Tagging Process
1. New images are pulled from database
2. Images are downloaded and sent to MITS Project compatible server (DeepBooru or IDEX+)
3. Rules are pulled and parsed
4. Tags are uploaded to database and linked to images
