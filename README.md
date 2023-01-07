# Mugino MIITS Client for Sequenzia IntelliDex+ 
<img width="700" src="https://user-images.githubusercontent.com/15165770/211075998-eef8e302-768c-4a78-8de3-fc287bade11d.png"><br/>
Mugino MIITS Project<br/>
Machine Intelligence Deep Image Tagging System (MIITS) Client for Sequenzia and IntelliDex

Simple wrapper for DeepDanbooru to process images from Sequenzia and retrieve tagging data async from normal operations.

## Tagging Process
1. New images are pulled from database
2. Images are downloaded and sent to an input folder
3. MIITS compatible system exports tags from images
4. Tags are parsed and uploaded to database and linked to images

## Important Tagging Warning
Sequenzia tagging is done "on the fly" to reduce the number of unnecessary tags that may never be used and just take up table space. Tag IDs WILL NOT match danbooru IDs! 
