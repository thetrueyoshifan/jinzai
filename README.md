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

## Example Configuration File - config.json
```json
{
  "system_name": "HOSTNAME",
  "sql_host": "",
  "mq_host": "",
  "log_host" : [],
  "sql_database": "kanmi_system",
  "sql_user": "",
  "sql_pass": "",
  "mq_user": "",
  "mq_pass": "",
  "mq_discord_out": "outbox.discord",
  "mq_mugino_in": "inbox.mugino",
  "search": [
    {
	  "class": ["review"],
	  "limit": 4000
    },
    {
	  "class": ["cosplay", "art", "photo"],
	  "limit": 5000
    }
  ],
  "rules": [
	{
		"channels": [
			"863820375723606046"
		]
	},
	{
		"channels": [
			"965828904629710878",
			"964390001104257044",
			"965828904629710878",
			"968624369456775258",
			"863820375723606046",
			"964390001104257044"
		],
		"block": [
			"no_humans",
			"user_interface",
			"phone_screen"
		]
	}
  ],
  "pull_limit": 4000,
  "parallel_downloads": 25,
  "deepbooru_exec": "C:\\Users\\ykaza\\AppData\\Local\\Programs\\Python\\Python37\\Scripts\\deepdanbooru.exe",
  "deepbooru_gpu": true,
  "deepbooru_model_path": "./model/",
  "deepbooru_input_path": "./temp/",
  "deepbooru_output_path": "./results/"
}
```
