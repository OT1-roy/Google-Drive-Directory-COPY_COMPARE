Google Drive Folder Compare Utility (compare.gs)

This script is a companion utility to the Drive folder copy system.
It performs deep, recursive comparisons between two Google Drive folders to detect:
	•	Missing files
	•	Extra files
	•	Mismatched files (by size and optional MD5 checksum)

It is ideal for verifying the integrity of large Google Drive copy operations after they complete.

⸻

Use Cases
	•	Post-copy verification between source and destination
	•	Spot-check large multi-GB or multi-folder migrations
	•	Logging any discrepancies for later inspection

⸻

How to Use

Note: This script must be placed in a separate Apps Script project from copy.gs.

	1.	Go to https://script.new to create a new Apps Script project.
	2.	Paste the contents of compare.gs into the editor.
	3.	Modify the constants at the top of the file:

const SOURCE_FOLDER_ID = 'your_source_folder_id';
const DESTINATION_PATH = 'Your/Destination/Path';


	4.	Run the function runCompareForCopyJob() manually.
	5.	Results will appear in the Logs and as a structured .json file saved to your logs folder.

⸻

Output

The comparison script logs and stores:
	•	Files present in source but missing in destination
	•	Files present in destination but not in source
	•	Files with matching names but different sizes or MD5 checksums

The results are written to a timestamped file in:

_CopyQueueData/logs/integrity_report_YYYY-MM-DD-HH-MM-SS.json



⸻

Technical Notes
	•	Up to 10 levels of folder depth are supported
	•	API rate limits are respected (with exponential backoff)
	•	File content is not compared directly — comparison is by metadata (name, size, optional MD5)

⸻

Limitations
	•	This does not copy or repair files — only logs discrepancies
	•	Cannot compare files in Trash or shared drives (unless explicitly provided access)
	•	Google Docs (Docs, Sheets, Slides) have no MD5 checksum available — only size will be used
	•	Large folders may take time to scan; the script auto-pauses to comply with quotas

⸻

License

This script is provided under the MIT License.
Use at your own risk — no warranty, no official support.

⸻

Author Notes

This tool was created as a utility for personal data verification, following large recursive copy operations on Google Drive.
It’s deliberately written to be readable, fault-tolerant, and useful for power users — not a polished product.
