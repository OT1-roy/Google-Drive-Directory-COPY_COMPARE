# Google Drive Folder Copy & Compare Utilities

A set of robust, fault-tolerant Google Apps Scripts for copying and verifying the contents of large Google Drive directories. Designed for reliability, scalability, and recoverability — tested with over 200GB of structured data.

> ⚠️ **Not a polished product** — but production-grade in robustness and suitable for power users or automation workflows.

---

## 📦 Repository Structure

google folder copy/
├── copy.gs        → Google Apps Script to recursively copy folder contents
├── README.md      → Detailed usage for the copy script

google folder compare/
├── compare.gs     → Script to compare directory contents using checksums
├── README.md      → Details for folder comparison logic

---

## ✨ Features

- Recursively copy nested Google Drive folders (up to 10 levels)
- Deduplicate using filename, size, and optionally MD5
- Resumable — all state saved to JSON
- Error recovery & retry logic for network/API quota issues
- Progress tracking via logs + email
- Script modularized into copy and compare utilities

---

## 📖 How to Use

1. Visit [https://script.new](https://script.new) to create a new Google Apps Script project.
2. Copy the contents of either `copy.gs` or `compare.gs` depending on your needs.
3. Follow each folder’s `README.md` for configuration & usage instructions.

---

## 🛠️ Contributing

Pull requests are welcome! To suggest changes or improvements:

- Fork the repo
- Create a new branch
- Submit a PR with clear explanation

---

## 📜 License

MIT — feel free to use, fork, and adapt.

---

## 🧾 Author Notes

Originally created by [OT1-Roy](https://github.com/OT1-roy) for personal data migration and verification at scale. Shared in the hope it is helpful to others managing large data operations on Google Drive.