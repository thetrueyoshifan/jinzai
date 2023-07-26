# Jinzai Consolidation and Backup System for Sequenzia
<!--<img width="700" src="https://user-images.githubusercontent.com/15165770/211075998-eef8e302-768c-4a78-8de3-fc287bade11d.png"><br/>!-->
Jinzai Project<br/>
Jinzai Image Consolidation and Data Backup System for Shutaura and Sequenzia

Project designed to deduplicate images stored in Shutaura, as well as prep all data for backing up to Tape, Blu-Ray, or raw FS.

## Requirements
- Exported ports from MQ and SQL
- Twice the storage space in your working directory than your current capacity (this accounts for joining files)

## Deduplication process
1. Images are pulled from selected channels and imported into temp directories
2. First pass checks the MD5 of all loaded images for exact matches
3. Second pass uses dupe-images to check the images via jimp for similarities
4. Duplicate images are marked for archival via MQ

## Backup process
1. All files are pulled from selected channels and imported into temp directories
2. Temp directories are passed through to ```tar```, ``cdrecord``, or stdout depending on the input parameters
3. Optionally, directory structure is passed through gzip for compression before archival

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
  "search": [
    {
	  "class": ["review"]
    },
    {
	  "class": ["cosplay", "art", "photo"]
    }
  ],
  "parallel_downloads": 25,
}
```

*This project is currently heavily WIP!*
