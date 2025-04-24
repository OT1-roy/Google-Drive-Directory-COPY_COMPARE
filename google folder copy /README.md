/**
 * Google Drive Folder Copy Utility
 * 
 * Description:
 *   This Google Apps Script recursively copies the full contents of a Google Drive folder
 *   into another location. It is robust, fault-tolerant, and designed to work with very large
 *   folder structures, handling chunked processing and retries automatically.
 * 
 * Author: Roy Hua (OT1-Roy)
 * License: MIT License
 * Version: 1.0
 * 
 * Disclaimer:
 *   This tool was developed by a non-professional programmer for personal data handling.
 *   It prioritizes reliability and recovery over performance or elegance.
 *   It is not officially supported, and is provided as-is, with no warranty.
 * 
 * Features:
 *   - Recursively copies up to 10 levels deep in Google Drive
 *   - Deduplicates files using name and size (optionally MD5)
 *   - Automatically splits work into batches to avoid 6-minute limit
 *   - Recovers from network/API errors with retries and state saving
 *   - Sends email notifications for job progress
 *   - Logs operations to JSON and log files
 *   - Successfully tested with over 200GB of data and deep folder structures
 * 
 * Setup:
 *   1. Go to https://script.new to create a new Apps Script project
 *   2. Paste this script and set your SOURCE_FOLDER_ID and DESTINATION_PATH
 *   3. Run initializeJob() manually to start
 */

Google Drive Folder Copy Utility (copy.gs)

This script is a robust, fault-tolerant Google Apps Script for recursively copying the full contents of a Google Drive folder into another location, preserving structure and avoiding duplicates.

Important: This script is designed to run independently.
It must be used in a separate Apps Script project from compare.gs.

⸻

Features
	•	Deep recursive folder copying (up to 10 levels)
	•	Deduplication using file name, size, and optionally MD5 checksum
	•	Chunked processing with queueing to avoid timeouts and API limits
	•	Automatic retries for transient errors (network, quota)
	•	State is saved in JSON for pause/resume or recovery
	•	Logs every operation into structured files
	•	Email notifications at key stages
	•	Includes UI menu if attached to a Google Sheet

⸻

How to Use
	1.	Go to https://script.new to create a new Google Apps Script project.
	2.	Paste the contents of copy.gs into the project.
	3.	Modify these configuration constants at the top of the file:

const SOURCE_FOLDER_ID = 'your_source_folder_id';
const DESTINATION_PATH = 'Your/Destination/Path';


	4.	Save and run the function initializeJob() manually the first time.
	5.	The job will continue in batches via triggers (processNextBatch).
	6.	You can monitor progress by calling getJobStatus() or showJobStatus().

⸻

Folder Structure

This script creates a structure like the following:

DESTINATION_PATH/
└── [source folder name]/
    └── [copied contents]

_CopyQueueData/
├── json_files/     ← stores queue, metadata, failed items
├── logs/           ← timestamped log files
└── combined_log_dump.txt



⸻

Configuration Highlights

All config variables are at the top of the script. Notable options:
	•	STRICT_DEDUPLICATION — skip files already copied (checks name + size/date)
	•	COPY_TRIGGER_MINUTES — interval between copy batches
	•	MAX_QUEUE_CHUNK_SIZE — size of file batches per trigger
	•	EMAIL_NOTIFICATION_INTERVAL — how often to send email updates

⸻

Resuming and Maintenance

This script supports error recovery and job resumption:
	•	resumeFailedCopies() — re-add failed items to queue
	•	requeueFailedItems() — manual retry of previously failed files
	•	recoverStuckJob() — force recovery if a batch is stuck
	•	resetJob() — start over from scratch

⸻

Limitations
	•	Cannot copy Google Forms (API limitation)
	•	Google Apps Script has a 6-minute limit per execution, but this script is designed to handle that:
	•	It automatically splits work into small batches
	•	Triggers re-run the job every few minutes
	•	It has successfully handled directories exceeding 200GB and deep, complex folder structures
	•	Subject to Google Drive quota limits (daily + per-minute)
	•	Preserving modification dates requires enabling the Advanced Drive API

⸻

Notifications

The script sends email updates to your account at key milestones:
	•	Copy job start
	•	Queue build completion
	•	Periodic status updates
	•	Job completed
	•	On errors or quota issues

⸻

License

This script is provided under the MIT License.
Use at your own risk — no warranty, no official support.

⸻

Author Notes

This was created by a non-professional programmer for personal use.
It emphasizes robustness over elegance.
Feel free to fork or adapt as needed.
