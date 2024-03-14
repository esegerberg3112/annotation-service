Each utility is in it's own subdirectory, and does the following:

/archive
* `archive.py` - Archives free user result files to Glacier
* `archive_config.ini` - Configuration options for archive utility

/restore
* `restore.py` - Initiates restore of Glacier archive(s)
* `restore_config.ini` - Configuration options for restore utility

/thaw
* `thaw.py` - Saves recently restored archive(s) to S3
* `thaw_config.ini` - Configuration options for thaw utility
