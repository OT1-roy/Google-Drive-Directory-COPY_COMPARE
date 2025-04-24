// === CONFIGURATION ===
const SOURCE_FOLDER_ID = 'your_source_folder_id';
const DESTINATION_PATH = 'Your/Destination/Path';
const SETUP_TRIGGER_MINUTES = 5;
const COPY_TRIGGER_MINUTES = 5;
const MAX_RETRIES = 3;
const NOTIFICATION_EMAIL = Session.getActiveUser().getEmail();
const MAX_RUNTIME_MS = 300000; // 5 minutes in milliseconds (to avoid 6-minute execution limit)
const SAFETY_BUFFER_MS = 60000; // 1 minute safety buffer to ensure graceful exit before time limit
const QUEUE_FOLDER_NAME = '_CopyQueueData';
const LOCK_TIMEOUT_MS = 420000; // 7 minutes (420,000 ms), adjusted for 6 min script limit
const SAVE_FREQUENCY = 20; // Save to Drive less frequently to reduce I/O overhead
const STRICT_DEDUPLICATION = true; // Use stricter deduplication rules (name + size/date)
const PRESERVE_DATES = false; // Disabled date preservation to avoid Drive.Files.patch errors
const EMAIL_NOTIFICATION_INTERVAL = 15; // Minutes between status email notifications
// Adaptive chunk size based on directory size
let MAX_QUEUE_CHUNK_SIZE = 1000; // Default chunk size, will be adjusted dynamically
const API_RATE_LIMIT_PAUSE = 1000; // Pause in ms when approaching rate limits
const AUTO_RETRY_FAILED_ITEMS = true; // Automatically retry failed items at end of batch

// Add constants for JSON organization
const JSON_FOLDER_NAME = 'json_files';
const LOGS_FOLDER_NAME = 'logs';

// Add constants for queue management
const QUEUE_CHUNK_PREFIX = 'queue_chunk_';

// Add max traversal depth constant
const MAX_TRAVERSAL_DEPTH = 10; // Increased from 7 to handle deeper folder structures

// === API QUOTA & RATE LIMITING ===
let apiCallCount = 0;
let apiCallStartTime = Date.now();
let apiQuotaStatus = { minute: 0, day: 0, lastPause: 0 };
const API_PER_MINUTE_LIMIT = 9000; // Lower than 12k/60s for safety
function checkApiRateLimit() {
  const now = Date.now();
  
  // Reset per-minute count
  if (now - apiCallStartTime > 60000) {
    apiCallCount = 0;
    apiCallStartTime = now;
  }
  
  apiCallCount++;

  // Check only per-minute limit
  if (apiCallCount > API_PER_MINUTE_LIMIT) {
    Logger.log(`🚦 Approaching per-minute API limit (${apiCallCount}/${API_PER_MINUTE_LIMIT}). Pausing for 10s.`);
    Utilities.sleep(10000); // Sleep longer if approaching per-minute limit
    // Reset count after pause to avoid immediate re-triggering
    apiCallCount = 0;
    apiCallStartTime = Date.now();
    return true; // Indicate that a pause occurred
  }
  
  return false; // Indicate no pause occurred
}

// === EMAIL NOTIFICATION THROTTLING ===
function shouldSendNotification() {
  const scriptProps = PropertiesService.getScriptProperties();
  const lastNotificationTime = scriptProps.getProperty('LAST_EMAIL_NOTIFICATION_TIME');
  
  if (!lastNotificationTime) {
    scriptProps.setProperty('LAST_EMAIL_NOTIFICATION_TIME', new Date().toISOString());
    return true;
  }
  
  const lastTime = new Date(lastNotificationTime).getTime();
  const currentTime = new Date().getTime();
  const minutesSinceLastEmail = (currentTime - lastTime) / (1000 * 60);
  
  // Only send email if enough time has passed
  if (minutesSinceLastEmail >= EMAIL_NOTIFICATION_INTERVAL) {
    scriptProps.setProperty('LAST_EMAIL_NOTIFICATION_TIME', new Date().toISOString());
    return true;
  }
  
  return false;
}

// === ENTRY POINT ===
function initializeJob() {
  try {
    // Clean up any existing triggers and properties
    cleanupAllTimeTriggers();
    PropertiesService.getScriptProperties().deleteAllProperties();
    
    // If preserving dates, enable Drive API
    if (PRESERVE_DATES) {
      enableDriveApiAccess();
    }
    
    // Get source and destination folders with permission checking
    let sourceFolder;
    try {
      sourceFolder = DriveApp.getFolderById(SOURCE_FOLDER_ID);
      // Test if we can actually read the source folder
      sourceFolder.getName(); // Will throw error if no access
      const fileIterator = sourceFolder.getFiles();
      if (fileIterator.hasNext()) {
        fileIterator.next(); // Test file access
      }
    } catch (e) {
      throw new Error(`Cannot access source folder: ${e.message}. Check that the folder exists and you have permission to read it.`);
    }
    
    const destRootFolder = getFolderByPath(DESTINATION_PATH);
    
    // Create a subfolder in the destination that matches the source folder name
    let destFolder;
    const sourceFolderName = sourceFolder.getName();
    const existingFolders = destRootFolder.getFoldersByName(sourceFolderName);
    
    if (existingFolders.hasNext()) {
      destFolder = existingFolders.next();
    } else {
      destFolder = destRootFolder.createFolder(sourceFolderName);
    }
    
    // Check for existing queue folder, create if not exists
    let queueFolder;
    const existingQueueFolders = DriveApp.getFoldersByName(QUEUE_FOLDER_NAME);
    if (existingQueueFolders.hasNext()) {
      queueFolder = existingQueueFolders.next();
    } else {
      queueFolder = DriveApp.createFolder(QUEUE_FOLDER_NAME);
    }
    
    // Create the JSON files subfolder
    let jsonFolder;
    const existingJsonFolders = queueFolder.getFoldersByName(JSON_FOLDER_NAME);
    if (existingJsonFolders.hasNext()) {
      jsonFolder = existingJsonFolders.next();
    } else {
      jsonFolder = queueFolder.createFolder(JSON_FOLDER_NAME);
    }
    
    // Create the logs subfolder
    let logsFolder;
    const existingLogsFolders = queueFolder.getFoldersByName(LOGS_FOLDER_NAME);
    if (existingLogsFolders.hasNext()) {
      logsFolder = existingLogsFolders.next();
    } else {
      logsFolder = queueFolder.createFolder(LOGS_FOLDER_NAME);
    }
    
    // Initialize metadata (add bytes tracking)
    const metadata = {
      startTime: new Date().toISOString(),
      totalItems: 0,
      processedItems: 0,
      failedItems: 0,
      totalBytes: 0,
      processedBytes: 0,
      failedBytes: 0,
      status: 'BUILDING_QUEUE',
      sourceFolderId: sourceFolder.getId(),
      sourceFolderName: sourceFolder.getName(),
      destFolderId: destFolder.getId()
    };
    // Save initial empty files
    saveJsonToFile(jsonFolder, 'queue.json', []);
    // Save initial pending folder with depth tracking
    saveJsonToFile(jsonFolder, 'pending_folders.json', [{
      id: sourceFolder.getId(),
      destId: destFolder.getId(),
      processed: false,
      path: '/' + sourceFolder.getName(),
      depth: 0
    }]);
    saveJsonToFile(jsonFolder, 'metadata.json', metadata);
    saveJsonToFile(jsonFolder, 'failed_items.json', []);
    
    // Save queue folder ID to properties for later use
    PropertiesService.getScriptProperties().setProperty('QUEUE_FOLDER_ID', queueFolder.getId());
    PropertiesService.getScriptProperties().setProperty('JSON_FOLDER_ID', jsonFolder.getId());
    PropertiesService.getScriptProperties().setProperty('LOGS_FOLDER_ID', logsFolder.getId());
    
    // Create trigger to start building the queue
    ScriptApp.newTrigger('processNextBatch')
      .timeBased()
      .everyMinutes(SETUP_TRIGGER_MINUTES)
      .create();
    
    // Log the initialization
    logOperation(logsFolder, 'JOB_INITIALIZED', {
      sourceFolderId: sourceFolder.getId(),
      sourceFolderName: sourceFolder.getName(),
      destFolderId: destFolder.getId(),
      timestamp: new Date().toISOString()
    });
    
    Logger.log(`✅ Job initialized. Building queue for ${sourceFolder.getName()}.`);
    sendEmailNotification('Copy Job Started', `Started building queue for ${sourceFolder.getName()}. You'll be notified when copying begins.`);
    return true;
  } catch (e) {
    Logger.log('❌ Error initializing job: ' + e.message);
    sendEmailNotification('Copy Job Error', 'Error initializing job: ' + e.message);
    return false;
  }
}

function processNextBatch() {
  // Smarter lock acquisition
  const lock = LockService.getScriptLock();
  const now = Date.now();
  const last = Number(PropertiesService.getScriptProperties().getProperty('LAST_START_TIME') || 0);
  
  // abort only if another run started < 5 min 40 s ago **and** the lock is still held
  if (!lock.tryLock(0) && (now - last) < 340000) {
    Logger.log('🚨 Lock failed: another batch is still active');  
    return;
  }
  PropertiesService.getScriptProperties().setProperty('LAST_START_TIME', String(now));
  
  const startTime = new Date().getTime();
  const currentDate = new Date().toISOString();
  
  // Check for potentially stuck previous execution
  checkAndClearStuckExecution();
  
  // Set running flag and timestamp
  PropertiesService.getScriptProperties().setProperty('IS_CURRENTLY_RUNNING', 'true');
  PropertiesService.getScriptProperties().setProperty('LAST_START_TIME', currentDate);
  
  try {
    // Get the queue folder
    const queueFolderId = PropertiesService.getScriptProperties().getProperty('QUEUE_FOLDER_ID');
    if (!queueFolderId) {
      throw new Error('Queue folder ID not found in properties');
    }
    const queueFolder = DriveApp.getFolderById(queueFolderId);
    
    // Get the JSON folder
    const jsonFolderId = PropertiesService.getScriptProperties().getProperty('JSON_FOLDER_ID');
    if (!jsonFolderId) {
      throw new Error('JSON folder ID not found in properties');
    }
    const jsonFolder = DriveApp.getFolderById(jsonFolderId);
    
    // Get current metadata
    const metadata = loadJsonFromFile(jsonFolder, 'metadata.json');
    if (!metadata) {
      throw new Error('Metadata not found');
    }
    
    // Continue based on job status
    if (metadata.status === 'BUILDING_QUEUE') {
      continueQueueBuilding(jsonFolder, metadata);
    } else if (metadata.status === 'COPYING') {
      continueCopying(jsonFolder, metadata);
    } else {
      Logger.log(`⚠️ No action taken. Job status: ${metadata.status}`);
    }
    
  } catch (e) {
    Logger.log('❌ Error in process batch: ' + e.message);
    sendEmailNotification('Copy Process Error', 'Error: ' + e.message);
  } finally {
    // Clear running flag
    PropertiesService.getScriptProperties().setProperty('IS_CURRENTLY_RUNNING', 'false');
    PropertiesService.getScriptProperties().setProperty('LAST_COMPLETED_TIME', new Date().toISOString());
    
    // Release the lock
    try { lock.releaseLock(); }
    catch (e) { Logger.log(`⚠️ Failed to release lock: ${e.message}`); }
    
    // Log execution time
    const executionTime = (new Date().getTime() - startTime) / 1000;
    Logger.log(`⏱️ Execution completed in ${executionTime.toFixed(2)} seconds`);
  }
}

function continueQueueBuilding(jsonFolder, metadata) {
  const startTime = new Date().getTime();
  let pendingFolders = [];
  let queue = [];
  // Validate and handle metadata status transitions
  if (metadata.status !== 'BUILDING_QUEUE') {
    Logger.log(`⚠️ Invalid status transition: Expected 'BUILDING_QUEUE', found '${metadata.status}'. Correcting.`);
    const logsFolderId = PropertiesService.getScriptProperties().getProperty('LOGS_FOLDER_ID');
    const logsFolder = logsFolderId ? DriveApp.getFolderById(logsFolderId) : null;
    if (logsFolder) {
      logOperation(logsFolder, 'STATUS_CORRECTION', {
        expectedStatus: 'BUILDING_QUEUE',
        actualStatus: metadata.status,
        action: 'CORRECTED',
        timestamp: new Date().toISOString()
      });
    }
    metadata.status = 'BUILDING_QUEUE';
    saveJsonToFile(jsonFolder, 'metadata.json', metadata);
  }
  
  // Dynamically adjust chunk size based on total items
  if (metadata.totalItems > 50000) {
    MAX_QUEUE_CHUNK_SIZE = 500; // Smaller chunks for very large directories
    Logger.log(`📊 Adjusted chunk size to ${MAX_QUEUE_CHUNK_SIZE} for large directory with ${metadata.totalItems} items`);
  } else if (metadata.totalItems > 10000) {
    MAX_QUEUE_CHUNK_SIZE = 750; // Medium chunks for medium directories
    Logger.log(`📊 Adjusted chunk size to ${MAX_QUEUE_CHUNK_SIZE} for medium directory with ${metadata.totalItems} items`);
  } else {
    MAX_QUEUE_CHUNK_SIZE = 1000; // Default for smaller directories
  }
  
  let logsFolder = null;
  try {
    const logsFolderId = PropertiesService.getScriptProperties().getProperty('LOGS_FOLDER_ID');
    if (logsFolderId) logsFolder = DriveApp.getFolderById(logsFolderId);
    pendingFolders = loadJsonFromFile(jsonFolder, 'pending_folders.json') || [];
    const queueChunks = getQueueChunks(jsonFolder);
    if (queueChunks.length > 0) {
      // Only track IDs for deduplication
      const existingItemIds = new Set();
      queueChunks.forEach(chunkInfo => {
        chunkInfo.sourceIds.forEach(id => existingItemIds.add(id));
      });
      Logger.log(`📊 Using ${queueChunks.length} existing queue chunks with ${existingItemIds.size} items`);
    } else {
      queue = loadJsonFromFile(jsonFolder, 'queue.json') || [];
      if (queue.length > MAX_QUEUE_CHUNK_SIZE) {
        createQueueChunks(jsonFolder, queue);
        queue = [];
      }
    }
    // Use a set to track file IDs for deduplication 
    const existingItemIds = new Set(queue.map(item => item.sourceId));
    queueChunks.forEach(chunk => {
      chunk.sourceIds.forEach(id => existingItemIds.add(id));
    });
    let newItems = [];
    let totalNewItems = 0;
    let totalNewBytes = 0;
    let foldersProcessed = 0;
    // Depth-limited traversal: process one folder at a time for incremental memory use
    while (pendingFolders.length > 0 && new Date().getTime() - startTime < MAX_RUNTIME_MS - SAFETY_BUFFER_MS) {
      const folderInfo = pendingFolders.shift();
      foldersProcessed++;
      // Track folder depth for traversal
      const folderDepth = folderInfo.depth || 0;

      // Declare files and subfolders for use outside of the try block
      let files = [];
      let subfolders = [];

      try {
        // Provide path context for structure preservation with depth
        const result = processFolderContents(folderInfo.id, folderInfo.destId, folderInfo.path || '', folderDepth);
        files = result.files;
        subfolders = result.subfolders;

        // Add files to queue, filtering duplicates
        if (files.length > 0) {
          const uniqueFiles = files.filter(file => !existingItemIds.has(file.sourceId));
          uniqueFiles.forEach(file => existingItemIds.add(file.sourceId));
          newItems = newItems.concat(uniqueFiles);
          totalNewItems += uniqueFiles.length;
          totalNewBytes += uniqueFiles.reduce((a, b) => a + (b.size || 0), 0);
        }
        // Add subfolders to pending list, enforcing depth limit
        if (subfolders.length > 0 && folderDepth < MAX_TRAVERSAL_DEPTH) {
          pendingFolders.push(...subfolders);
        }
        if (logsFolder) {
          logOperation(logsFolder, 'PROCESS_FOLDER', {
            folderId: folderInfo.id,
            filesFound: files.length,
            subfoldersFound: subfolders.length,
            path: folderInfo.path
          });
        }
      } catch (folderError) {
        Logger.log(`❌ Error processing folder ${folderInfo.id}: ${folderError.message}`);
        if (logsFolder) {
          logOperation(logsFolder, 'FOLDER_ERROR', {
            folderId: folderInfo.id,
            error: folderError.message,
            path: folderInfo.path
          });
        }
      }
      // --- More Aggressive State Saving ---
      const timeElapsedMs = new Date().getTime() - startTime;
      // Save based on frequency OR if nearing the 6-minute limit (e.g., after 4 mins)
      const shouldSaveFolderCount = foldersProcessed % SAVE_FREQUENCY === 0;
      const shouldSaveTimeBased = timeElapsedMs > 240000; // 4 minutes (240,000 ms)

      if (shouldSaveFolderCount || shouldSaveTimeBased) {
        if (newItems.length > 0) {
          // Decide how to save new items: append to queue.json or add to chunks
          if (queue.length + newItems.length > MAX_QUEUE_CHUNK_SIZE) {
            if (queue.length > 0) {
              addItemsToQueueChunks(jsonFolder, queue);
              queue = [];
            }
            addItemsToQueueChunks(jsonFolder, newItems);
          } else {
            queue = queue.concat(newItems);
            saveJsonToFile(jsonFolder, 'queue.json', queue); // Save queue if small enough
          }
          newItems = []; // Clear newItems after saving
        }
        saveJsonToFile(jsonFolder, 'pending_folders.json', pendingFolders);
        if (logsFolder) {
          logOperation(logsFolder, 'PERIODIC_SAVE', {
            reason: shouldSaveFolderCount ? `Frequency (${SAVE_FREQUENCY})` : 'Time (4min)',
            foldersProcessed: foldersProcessed,
            pendingFoldersCount: pendingFolders.length,
            timestamp: new Date().toISOString()
          });
        }
      }
      
      // Enhanced checkpointing for deep folders or folders with many files
      if (folderInfo.depth > 2 || files.length > 500) {
        if (newItems.length > 0) {
          queue = queue.concat(newItems);
          saveJsonToFile(jsonFolder, 'queue.json', queue);
          newItems = [];
        }
        saveJsonToFile(jsonFolder, 'pending_folders.json', pendingFolders);
        if (logsFolder) {
          logOperation(logsFolder, 'ENHANCED_CHECKPOINT', {
            folderDepth: folderInfo.depth,
            filesInFolder: files.length,
            pendingFoldersCount: pendingFolders.length,
            timestamp: new Date().toISOString()
          });
        }
      }
    }
    // Save final state - handle large queues
    if (newItems.length > 0) {
      if (queue.length + newItems.length > MAX_QUEUE_CHUNK_SIZE) {
        if (queue.length > 0) {
          addItemsToQueueChunks(jsonFolder, queue);
          queue = [];
        }
        addItemsToQueueChunks(jsonFolder, newItems);
      } else {
        queue = queue.concat(newItems);
        saveJsonToFile(jsonFolder, 'queue.json', queue);
      }
    }
    saveJsonToFile(jsonFolder, 'pending_folders.json', pendingFolders);
    metadata.totalItems += totalNewItems;
    metadata.totalBytes = (metadata.totalBytes || 0) + totalNewBytes;
    if (pendingFolders.length === 0) {
      metadata.status = 'COPYING';
      Logger.log(`✅ Queue building complete. ${metadata.totalItems} items to copy, ${metadata.totalBytes} bytes.`);
      sendEmailNotification('Queue Building Complete', 
                          `Queue building completed with ${metadata.totalItems} items (${metadata.totalBytes} bytes) to copy. Copying will begin every ${COPY_TRIGGER_MINUTES} minutes.`);
    } else {
      Logger.log(`📊 Queue building progress: ${metadata.totalItems} items (${metadata.totalBytes} bytes) so far, ${pendingFolders.length} folders remaining.`);
    }
    saveJsonToFile(jsonFolder, 'metadata.json', metadata);
    if (logsFolder) {
      logOperation(logsFolder, 'QUEUE_BUILD_PROGRESS', {
        totalItems: metadata.totalItems,
        totalBytes: metadata.totalBytes,
        newItemsAdded: totalNewItems,
        newBytesAdded: totalNewBytes,
        pendingFolders: pendingFolders.length,
        status: metadata.status
      });
    }
  } catch (e) {
    Logger.log('❌ Error building queue: ' + e.message);
    sendEmailNotification('Queue Building Error', 'Error: ' + e.message);
  }
}

// Enhanced: Accepts `path` and `depth` for full folder path tracking, and batches/prioritizes small files first
function processFolderContents(folderId, destFolderId, folderPath, depth = 0) {
  checkApiRateLimit();
  const sourceFolder = DriveApp.getFolderById(folderId);
  const destFolder = DriveApp.getFolderById(destFolderId);
  const files = [];
  const subfolders = [];
  try {
    // Incremental processing: only process up to 250 files at a time
    let fileCount = 0;
    let fileIterator = sourceFolder.getFiles();
    let fileBatch = [];
    const MAX_FILES_PER_FOLDER_VISIT = 500; 
    while (fileIterator.hasNext() && fileCount < MAX_FILES_PER_FOLDER_VISIT) {
      try {
        checkApiRateLimit();
        const file = fileIterator.next();
        fileCount++;
        // Gather file info for batching/sorting
        fileBatch.push({
          type: 'file',
          name: file.getName(),
          sourceId: file.getId(),
          destParentId: destFolder.getId(),
          size: file.getSize(),
          mimeType: file.getMimeType(),
          dateCreated: file.getDateCreated().toISOString(),
          md5: file.getMd5Checksum ? file.getMd5Checksum() : null,
          path: (folderPath || '') + '/' + file.getName()
        });
      } catch (e) {
        Logger.log(`⚠️ Error processing file in ${sourceFolder.getName()}: ${e.message}`);
      }
      if (fileCount % 250 === 0) {
        Utilities.sleep(100);
      }
    }
    // Smarter batching: prioritize small files first (<10MB), then medium, then large
    fileBatch.sort((a, b) => a.size - b.size);
    for (let fileObj of fileBatch) {
      if (shouldCopyFile(DriveApp.getFileById(fileObj.sourceId), destFolder)) {
        files.push(fileObj);
      }
    }
    if (fileCount >= MAX_FILES_PER_FOLDER_VISIT) {
      Logger.log(`⚠️ Folder has ${MAX_FILES_PER_FOLDER_VISIT}+ files, processing in batches: ${sourceFolder.getName()}`);
    }
  } catch (e) {
    Logger.log(`❌ Error listing files in folder: ${e.message}`);
  }
  try {
    // Process subfolders in batches, add full path and depth
    let folderCount = 0;
    let folderIterator = sourceFolder.getFolders();
    // Reduce max subfolders processed per folder visit
    const MAX_FOLDERS_PER_VISIT = 500;
    while (folderIterator.hasNext() && folderCount < MAX_FOLDERS_PER_VISIT) {
      const subfolder = folderIterator.next();
      folderCount++;
      let newDestFolder = null;
      const existingFolders = destFolder.getFoldersByName(subfolder.getName());
      if (existingFolders.hasNext()) {
        newDestFolder = existingFolders.next();
      } else {
        newDestFolder = destFolder.createFolder(subfolder.getName());
      }
      subfolders.push({
        id: subfolder.getId(),
        destId: newDestFolder.getId(),
        processed: false,
        path: (folderPath || '') + '/' + subfolder.getName(),
        depth: depth + 1
      });
      if (folderCount % 100 === 0) { // Sleep less often
        Utilities.sleep(50); // Sleep for shorter duration
      }
    }
    if (folderCount >= MAX_FOLDERS_PER_VISIT) {
      Logger.log(`⚠️ Folder has ${MAX_FOLDERS_PER_VISIT}+ subfolders, processing in batches: ${sourceFolder.getName()}`);
    }
  } catch (e) {
    Logger.log(`❌ Error listing subfolders in folder: ${e.message}`);
  }
  return { files, subfolders };
}

// Enhanced deduplication check
function shouldCopyFile(sourceFile, destFolder) {
  const fileName = sourceFile.getName();
  const existingFiles = destFolder.getFilesByName(fileName);
  
  // If no existing files with this name, we should copy
  if (!existingFiles.hasNext()) {
    return true;
  }
  
  // If not using strict deduplication, stop at name check
  if (!STRICT_DEDUPLICATION) {
    return false;
  }
  
  // With strict deduplication, we also check size and modified date
  try {
    const sourceSize = sourceFile.getSize();
    const sourceDate = sourceFile.getLastUpdated();
    
    // Check all existing files with the same name
    while (existingFiles.hasNext()) {
      const existingFile = existingFiles.next();
      
      // First check size - only compute MD5 if size differs
      const sizeMatch = existingFile.getSize() === sourceSize;
      if (!sizeMatch && sourceFile.getMd5Checksum) {
        const sourceMD5 = sourceFile.getMd5Checksum();
        const existingMD5 = existingFile.getMd5Checksum();
        if (sourceMD5 && existingMD5 && sourceMD5 === existingMD5) {
          return false;
        }
      }
      
      // If size matches, check modified date (with a small tolerance)
      if (sizeMatch) {
        const dateMatch = Math.abs(existingFile.getLastUpdated() - sourceDate) < 2000; // 2 seconds tolerance
        if (dateMatch) {
          return false;
        }
      }
    }
    
    // No matching files found, should copy
    return true;
  } catch (e) {
    // If error during comparison, default to copying (safer)
    Logger.log(`Warning: Error during file comparison for ${fileName}: ${e.message}`);
    return true;
  }
}

function continueCopying(jsonFolder, metadata) {
  // Validate and handle metadata status transitions
  if (metadata.status !== 'COPYING') {
    Logger.log(`⚠️ Invalid status transition: Expected 'COPYING', found '${metadata.status}'. Correcting.`);
    const logsFolderId = PropertiesService.getScriptProperties().getProperty('LOGS_FOLDER_ID');
    const logsFolder = logsFolderId ? DriveApp.getFolderById(logsFolderId) : null;
    if (logsFolder) {
      logOperation(logsFolder, 'STATUS_CORRECTION', {
        expectedStatus: 'COPYING',
        actualStatus: metadata.status,
        action: 'CORRECTED',
        timestamp: new Date().toISOString()
      });
    }
    metadata.status = 'COPYING';
    saveJsonToFile(jsonFolder, 'metadata.json', metadata);
  }
  
  // Dynamically adjust chunk size based on total items and average file size
  if (metadata.totalItems > 50000) {
    MAX_QUEUE_CHUNK_SIZE = 500; // Smaller chunks for very large directories
    Logger.log(`📊 Adjusted chunk size to ${MAX_QUEUE_CHUNK_SIZE} for large directory with ${metadata.totalItems} items`);
  } else if (metadata.totalItems > 10000) {
    MAX_QUEUE_CHUNK_SIZE = 750; // Medium chunks for medium directories
    Logger.log(`📊 Adjusted chunk size to ${MAX_QUEUE_CHUNK_SIZE} for medium directory with ${metadata.totalItems} items`);
  } else {
    MAX_QUEUE_CHUNK_SIZE = 1000; // Default for smaller directories
  }
  
  // If average file size is large, use even smaller chunks
  if (metadata.totalItems > 0 && metadata.totalBytes > 0) {
    const avgFileSize = metadata.totalBytes / metadata.totalItems;
    if (avgFileSize > 50 * 1024 * 1024) { // Average > 50MB
      MAX_QUEUE_CHUNK_SIZE = Math.min(MAX_QUEUE_CHUNK_SIZE, 300);
      Logger.log(`📊 Further reduced chunk size to ${MAX_QUEUE_CHUNK_SIZE} due to large average file size (${Math.round(avgFileSize/1024/1024)}MB)`);
    }
  }
  
  const startTime = new Date().getTime();
  let queue = [];
  let failedItems = [];
  let itemsCopied = 0;
  let itemsFailed = 0;
  let bytesCopied = 0;
  let bytesFailed = 0;
  let skippedItems = 0;
  let lastSaveTime = startTime;
  try {
    failedItems = loadJsonFromFile(jsonFolder, 'failed_items.json') || [];
    const chunkInfo = getQueueChunks(jsonFolder);
    let activeChunk = null;
    if (chunkInfo.length > 0) {
      activeChunk = chunkInfo.find(chunk => !chunk.isProcessed);
      if (activeChunk) {
        const chunkFile = jsonFolder.getFilesByName(activeChunk.filename);
        if (chunkFile.hasNext()) {
          queue = JSON.parse(chunkFile.next().getBlob().getDataAsString()) || [];
        }
        Logger.log(`📊 Processing queue chunk ${activeChunk.index + 1}/${chunkInfo.length} with ${queue.length} items`);
      } else {
        queue = loadJsonFromFile(jsonFolder, 'queue.json') || [];
      }
    } else {
      queue = loadJsonFromFile(jsonFolder, 'queue.json') || [];
    }
    // Smarter batching: process small files first
    queue.sort((a, b) => (a.size || 0) - (b.size || 0));
    const driveApiAvailable = PRESERVE_DATES && isDriveApiEnabled();
    while (queue.length > 0 && new Date().getTime() - startTime < MAX_RUNTIME_MS - SAFETY_BUFFER_MS) {
      const item = queue.shift();
      let success = false;
      let errorClass = 'unknown';
      for (let retry = 0; retry < MAX_RETRIES && !success; retry++) {
        try {
          if (item.type === 'file') {
            checkApiRateLimit();
            const sourceFile = DriveApp.getFileById(item.sourceId);
            const destFolder = DriveApp.getFolderById(item.destParentId);

            // fast-dedup path
            if (!shouldCopyFile(sourceFile, destFolder)) {
              skippedItems++;
              success = true;
              break;
            }

            // copy required
            const originalModifiedDate = sourceFile.getLastUpdated();
            checkApiRateLimit();
            const newFile = sourceFile.makeCopy(item.name, destFolder);
            if (driveApiAvailable && PRESERVE_DATES) {
              try {
                checkApiRateLimit();
                preserveFileDate(newFile.getId(), originalModifiedDate);
              } catch (dateErr) {
                Logger.log(`⚠️ Could not preserve date for ${item.name}: ${dateErr.message}`);
              }
            }
          }
          success = true;
          itemsCopied++;
          bytesCopied += item.size || 0;
        } catch (e) {
          // Handle daily quota exceeded: stop processing until next run
          if (e.message.includes('Daily Limit')) {
            // Re-queue current and remaining items for tomorrow
            queue.unshift(item);
            saveJsonToFile(jsonFolder, 'queue.json', queue);
            if (logsFolder) {
              logOperation(logsFolder, 'DAILY_LIMIT_REACHED', {
                error: e.message,
                timestamp: new Date().toISOString()
              });
            }
            return; // exit continueCopying early
          }
          // Classify other quota/rate limit errors
          if (e.message.match(/Rate Limit|quota|Limit Exceeded|userRateLimitExceeded/)) {
            errorClass = 'quota';
            const backoffTime = Math.pow(2, retry + 2) * 1000;
            Logger.log(`⚠️ Quota/rate limit error, backing off for ${backoffTime/1000} seconds`);
            Utilities.sleep(backoffTime);
            if (retry === MAX_RETRIES - 1) {
              failedItems.push({
                ...item,
                error: e.message,
                errorClass,
                timestamp: new Date().toISOString()
              });
              itemsFailed++;
              bytesFailed += item.size || 0;
              Logger.log(`❌ Quota error: Failed to copy after ${MAX_RETRIES} attempts: ${item.name} - ${e.message}`);
            }
          } else if (e.message.match(/Network|Timeout|service unavailable|fetch/)) {
            errorClass = 'network';
            Utilities.sleep(Math.pow(2, retry) * 1000);
            if (retry === MAX_RETRIES - 1) {
              failedItems.push({
                ...item,
                error: e.message,
                errorClass,
                timestamp: new Date().toISOString()
              });
              itemsFailed++;
              bytesFailed += item.size || 0;
              Logger.log(`❌ Network error: Failed to copy after ${MAX_RETRIES} attempts: ${item.name} - ${e.message}`);
            }
          } else if (e.message.match(/permission|Access denied|forbidden/i)) {
            errorClass = 'permission';
            // Don't retry permission errors
            failedItems.push({
              ...item,
              error: e.message,
              errorClass,
              timestamp: new Date().toISOString()
            });
            itemsFailed++;
            bytesFailed += item.size || 0;
            Logger.log(`❌ Permission error: ${item.name} - ${e.message}`);
            break;
          } else {
            errorClass = 'other';
            Utilities.sleep(Math.pow(2, retry) * 500);
            if (retry === MAX_RETRIES - 1) {
              failedItems.push({
                ...item,
                error: e.message,
                errorClass,
                timestamp: new Date().toISOString()
              });
              itemsFailed++;
              bytesFailed += item.size || 0;
              Logger.log(`❌ Other error: Failed to copy after ${MAX_RETRIES} attempts: ${item.name} - ${e.message}`);
            }
          }
        }
      }
      // Save progress periodically
      const currentTime = new Date().getTime();
      if (itemsCopied % SAVE_FREQUENCY === 0 || 
          currentTime - lastSaveTime > 30000 ||
          currentTime - startTime > (MAX_RUNTIME_MS - SAFETY_BUFFER_MS) / 2) {
        saveJsonToFile(jsonFolder, 'queue.json', queue);
        if (failedItems.length > 0) {
          saveJsonToFile(jsonFolder, 'failed_items.json', failedItems);
        }
        lastSaveTime = currentTime;
      }
    }
    // Update metadata
    metadata.processedItems += itemsCopied;
    metadata.failedItems += itemsFailed;
    metadata.processedBytes = (metadata.processedBytes || 0) + bytesCopied;
    metadata.failedBytes = (metadata.failedBytes || 0) + bytesFailed;
    metadata.skippedItems = (metadata.skippedItems || 0) + skippedItems;
    // Report progress by items and bytes
    reportProgress(metadata);
    const progress = metadata.totalItems > 0 ? 
                     Math.round((metadata.processedItems / metadata.totalItems) * 100) : 0;
    // Save final state
    saveJsonToFile(jsonFolder, 'queue.json', queue);
    if (failedItems.length > 0) {
      saveJsonToFile(jsonFolder, 'failed_items.json', failedItems);
      if (AUTO_RETRY_FAILED_ITEMS && queue.length === 0) {
        tryRequeueFailedItems(jsonFolder, failedItems);
      }
    }
    // Mark chunk as processed if using chunks and queue is empty
    if (activeChunk && queue.length === 0) {
      activeChunk.isProcessed = true;
      saveJsonToFile(jsonFolder, 'queue_chunks.json', chunkInfo);
    }

    if (queue.length === 0 && (chunkInfo.length === 0 || chunkInfo.every(c => c.isProcessed))) {
      metadata.status = 'COMPLETE';
      metadata.endTime = new Date().toISOString();
      finalizeJob(jsonFolder, metadata);
    } else {
      if (activeChunk) {
        saveJsonToFile(jsonFolder, activeChunk.filename, queue);
      } else {
        saveJsonToFile(jsonFolder, 'queue.json', queue);
      }
      // Report progress (item count and bytes)
      const totalRemaining = chunkInfo.reduce((total, chunk) => 
        total + (chunk.isProcessed ? 0 : chunk.itemCount), 0) + queue.length;
      const totalBytesRemaining = (chunkInfo.reduce((total, chunk) =>
        total + (chunk.isProcessed ? 0 : (chunk.totalBytes || 0)), 0)
        + queue.reduce((a, b) => a + (b.size || 0), 0));
      Logger.log(`📊 Copy progress: ${metadata.processedItems}/${metadata.totalItems} items, ${metadata.processedBytes}/${metadata.totalBytes} bytes. Failed: ${metadata.failedItems}, Remaining: ${totalRemaining} items, ${totalBytesRemaining} bytes`);
    }
    saveJsonToFile(jsonFolder, 'metadata.json', metadata);
  } catch (e) {
    Logger.log('❌ Error copying items: ' + e.message);
    sendEmailNotification('Copy Process Error', 'Error: ' + e.message);
  }
}

function finalizeJob(jsonFolder, metadata) {
  try {
    // Get logs folder for operation logging
    const logsFolderId = PropertiesService.getScriptProperties().getProperty('LOGS_FOLDER_ID');
    const logsFolder = logsFolderId ? DriveApp.getFolderById(logsFolderId) : null;
    
    const startTime = new Date(metadata.startTime);
    const endTime = new Date(metadata.endTime || new Date().toISOString());
    const durationMinutes = (endTime - startTime) / 1000 / 60;
    
    const skippedItems = (metadata.skippedItems !== undefined)
                       ? metadata.skippedItems
                       : metadata.totalItems - metadata.processedItems - metadata.failedItems;
    const summary = `
Copy Job Summary
---------------
Total Items: ${metadata.totalItems}
Successfully Copied: ${metadata.processedItems}
Skipped Items: ${skippedItems}
Failed Items: ${metadata.failedItems}
Duration: ${durationMinutes.toFixed(2)} minutes
Success Rate: ${((metadata.processedItems / metadata.totalItems) * 100).toFixed(1)}%
    `;
    
    Logger.log('✅ Copy job completed\n' + summary);
    sendEmailNotification('Copy Job Completed', summary);
    
    // Clean up triggers
    cleanupAllTimeTriggers();
    
    // Keep the queue folder for possible reprocessing of failed items
    // But save final metadata
    saveJsonToFile(jsonFolder, 'metadata.json', metadata);
    
    // Log the completion with skipped count
    if (logsFolder) {
      logOperation(logsFolder, 'JOB_COMPLETED', {
        totalItems: metadata.totalItems,
        successfulCopies: metadata.processedItems,
        skippedItems: skippedItems,
        failedItems: metadata.failedItems,
        duration: durationMinutes,
        successRate: ((metadata.processedItems / metadata.totalItems) * 100).toFixed(1),
        timestamp: new Date().toISOString()
      });
    }
  } catch (e) {
    Logger.log('❌ Error finalizing job: ' + e.message);
  }
}

// Modified to clean up all time-based triggers
function cleanupAllTimeTriggers() {
  ScriptApp.getProjectTriggers().forEach(trigger => {
    if (trigger.getEventType() === ScriptApp.EventType.CLOCK) {
      ScriptApp.deleteTrigger(trigger);
    }
  });
}

// === UTILITY FUNCTIONS ===

function saveJsonToFile(folder, filename, data) {
  try {
    const blob = Utilities.newBlob(JSON.stringify(data, null, 2), 'application/json');
    const existingFiles = folder.getFilesByName(filename);
    
    if (existingFiles.hasNext()) {
      const file = existingFiles.next();
      file.setContent(blob.getDataAsString());
      return file;
    } else {
      return folder.createFile(filename, blob.getDataAsString(), 'application/json');
    }
  } catch (e) {
    Logger.log(`❌ Error saving ${filename}: ${e.message}`);
    
    // Try to create a backup with simplified data
    try {
      const simplifiedData = (Array.isArray(data)) ? 
          { count: data.length, message: "Data was too large to save directly" } : 
          { message: "Data was too large to save directly" };
          
      const backupName = `${filename}.error.${new Date().getTime()}`;
      folder.createFile(backupName, JSON.stringify(simplifiedData), 'application/json');
      
      Logger.log(`⚠️ Created backup file ${backupName} with simplified data`);
    } catch (backupError) {
      Logger.log(`❌ Even backup creation failed: ${backupError.message}`);
    }
    
    return null;
  }
}

function loadJsonFromFile(folder, filename) {
  try {
    const files = folder.getFilesByName(filename);
    if (files.hasNext()) {
      const file = files.next();
      const content = file.getBlob().getDataAsString();
      
      try {
        return JSON.parse(content);
      } catch (parseError) {
        // Handle JSON parsing errors
        Logger.log(`❌ Error parsing JSON in ${filename}: ${parseError.message}`);
        
        // Create a backup of the corrupted file
        const backupName = `${filename}.backup.${new Date().getTime()}`;
        folder.createFile(backupName, content, 'application/json');
        
        // Return empty data based on filename
        if (filename === 'queue.json' || filename === 'failed_items.json' || 
            filename === 'pending_folders.json') {
          return [];
        } else if (filename === 'metadata.json') {
          return {
            startTime: new Date().toISOString(),
            totalItems: 0,
            processedItems: 0,
            failedItems: 0,
            status: 'ERROR',
            error: `JSON parsing error: ${parseError.message}`
          };
        }
        return null;
      }
    }
    return null;
  } catch (e) {
    Logger.log(`❌ Error loading ${filename}: ${e.message}`);
    return null;
  }
}

function getFolderByPath(path) {
  const parts = path.split('/');
  let folder = DriveApp.getRootFolder();
  for (const name of parts) {
    if (!name) continue;
    const folders = folder.getFoldersByName(name);
    folder = folders.hasNext() ? folders.next() : folder.createFolder(name);
  }
  return folder;
}

function sendEmailNotification(subject, body) {
  const key = 'LAST_EMAIL_NOTIFICATION';
  const now = Date.now();
  const last = Number(PropertiesService.getScriptProperties().getProperty(key) || 0);
  
  if (now - last >= EMAIL_NOTIFICATION_INTERVAL * 60000) {
    MailApp.sendEmail({
      to: NOTIFICATION_EMAIL,
      subject: `[Google Drive Copy] ${subject}`,
      body: body
    });
    PropertiesService.getScriptProperties().setProperty(key, String(now));
    Logger.log(`📧 Email notification sent: ${subject}`);
  } else {
    // Log without sending email
    Logger.log(`📧 Email notification throttled: ${subject}`);
  }
}

// === MANUAL OPERATIONS ===

function resetJob() {
  try {
    // Archive existing logs before reset
    const queueFolders = DriveApp.getFoldersByName(QUEUE_FOLDER_NAME);
    if (queueFolders.hasNext()) {
      const queueFolder = queueFolders.next();
      const logsFolders = queueFolder.getFoldersByName(LOGS_FOLDER_NAME);
      if (logsFolders.hasNext()) {
        const logsFolder = logsFolders.next();
        const archiveRoot = getFolderByPath('DriveCopyLogArchive');
        const filesIter = logsFolder.getFiles();
        while (filesIter.hasNext()) {
          const file = filesIter.next();
          file.makeCopy(`Archive_${file.getName()}`, archiveRoot);
        }
      }
      // Rename instead of trash
      queueFolder.setName(`_CopyQueueData_ARCHIVED_${Date.now()}`);
    }
    
    // Clean up any existing triggers
    cleanupAllTimeTriggers();
    
    // Clean up script properties
    PropertiesService.getScriptProperties().deleteAllProperties();
    
    Logger.log('🧹 Job reset successful. Ready to run initializeJob().');
  } catch (e) {
    Logger.log('❌ Error resetting job: ' + e.message);
  }
}

function resumeFailedCopies() {
  try {
    const queueFolderId = PropertiesService.getScriptProperties().getProperty('QUEUE_FOLDER_ID');
    if (!queueFolderId) {
      Logger.log('❌ No active job found. Please initialize a new job.');
      return;
    }
    
    const jsonFolderId = PropertiesService.getScriptProperties().getProperty('JSON_FOLDER_ID');
    if (!jsonFolderId) {
      Logger.log('❌ JSON folder not found. Please initialize a new job.');
      return;
    }
    
    const logsFolderId = PropertiesService.getScriptProperties().getProperty('LOGS_FOLDER_ID');
    const logsFolder = logsFolderId ? DriveApp.getFolderById(logsFolderId) : null;
    
    const jsonFolder = DriveApp.getFolderById(jsonFolderId);
    const metadata = loadJsonFromFile(jsonFolder, 'metadata.json');
    const failedItems = loadJsonFromFile(jsonFolder, 'failed_items.json') || [];
    
    if (failedItems.length === 0) {
      Logger.log('✅ No failed items to retry.');
      return;
    }
    
    // Move failed items back to queue
    let queue = loadJsonFromFile(jsonFolder, 'queue.json') || [];
    queue = failedItems.concat(queue);
    
    // Clear failed items
    saveJsonToFile(jsonFolder, 'queue.json', queue);
    saveJsonToFile(jsonFolder, 'failed_items.json', []);
    
    // Update metadata
    metadata.status = 'COPYING';
    metadata.totalItems = metadata.totalItems + metadata.failedItems - metadata.processedItems;
    metadata.processedItems = 0;
    metadata.failedItems = 0;
    saveJsonToFile(jsonFolder, 'metadata.json', metadata);
    
    // Create trigger if not exists
    const triggers = ScriptApp.getProjectTriggers();
    let triggerExists = false;
    triggers.forEach(trigger => {
      if (trigger.getEventType() === ScriptApp.EventType.CLOCK) {
        triggerExists = true;
      }
    });
    
    if (!triggerExists) {
      ScriptApp.newTrigger('processNextBatch')
        .timeBased()
        .everyMinutes(COPY_TRIGGER_MINUTES)
        .create();
    }
    
    Logger.log(`🔄 Resumed copying with ${queue.length} items.`);
    sendEmailNotification('Copy Job Resumed', `Retrying ${failedItems.length} failed items.`);
    
    // Log the resumption
    if (logsFolder) {
      logOperation(logsFolder, 'FAILED_COPIES_RESUMED', {
        failedItemsCount: failedItems.length,
        newQueueSize: queue.length,
        timestamp: new Date().toISOString()
      });
    }
  } catch (e) {
    Logger.log('❌ Error resuming failed copies: ' + e.message);
  }
}

function getJobStatus() {
  try {
    const scriptProps = PropertiesService.getScriptProperties();
    const isRunning = scriptProps.getProperty('IS_CURRENTLY_RUNNING') === 'true';
    const lastStartTime = scriptProps.getProperty('LAST_START_TIME');
    const queueFolderId = scriptProps.getProperty('QUEUE_FOLDER_ID');
    if (!queueFolderId) {
      return {
        status: 'NO_JOB',
        message: 'No active job found.'
      };
    }
    const jsonFolderId = scriptProps.getProperty('JSON_FOLDER_ID');
    if (!jsonFolderId) {
      return {
        status: 'ERROR',
        message: 'JSON folder not found for active job.'
      };
    }
    const jsonFolder = DriveApp.getFolderById(jsonFolderId);
    const metadata = loadJsonFromFile(jsonFolder, 'metadata.json');
    if (!metadata) {
      return {
        status: 'ERROR',
        message: 'Metadata not found for active job.'
      };
    }
    const queue = loadJsonFromFile(jsonFolder, 'queue.json') || [];
    const failedItems = loadJsonFromFile(jsonFolder, 'failed_items.json') || [];
    const pendingFolders = metadata.status === 'BUILDING_QUEUE' ? 
                          loadJsonFromFile(jsonFolder, 'pending_folders.json') || [] : 
                          [];
    // Calculate bytes progress
    const progress = metadata.totalItems > 0 ? 
                    Math.round((metadata.processedItems / metadata.totalItems) * 100) : 0;
    const bytesProgress = metadata.totalBytes > 0 ?
                    Math.round((metadata.processedBytes / metadata.totalBytes) * 100) : 0;
    const startTime = new Date(metadata.startTime);
    const endTime = metadata.endTime ? new Date(metadata.endTime) : null;
    const durationMinutes = endTime ? 
                          (endTime - startTime) / 1000 / 60 : 
                          (new Date() - startTime) / 1000 / 60;
    let potentiallyStuck = false;
    if (isRunning && lastStartTime) {
      const lastStartDate = new Date(lastStartTime);
      const minutesSinceStart = (new Date() - lastStartDate) / 1000 / 60;
      potentiallyStuck = minutesSinceStart > 10;
    }
    return {
      status: metadata.status,
      progress: progress,
      bytesProgress: bytesProgress,
      totalItems: metadata.totalItems,
      processedItems: metadata.processedItems,
      failedItems: metadata.failedItems,
      totalBytes: metadata.totalBytes,
      processedBytes: metadata.processedBytes,
      failedBytes: metadata.failedBytes,
      remainingItems: queue.length,
      pendingFolders: pendingFolders.length,
      startTime: metadata.startTime,
      endTime: metadata.endTime,
      durationMinutes: durationMinutes.toFixed(2),
      isCurrentlyRunning: isRunning,
      lastStartTime: lastStartTime,
      potentiallyStuck: potentiallyStuck
    };
  } catch (e) {
    Logger.log('❌ Error getting job status: ' + e.message);
    return {
      status: 'ERROR',
      message: 'Error: ' + e.message
    };
  }
}

// Function to clear a potentially stuck run
function clearStuckRun() {
  try {
    const scriptProps = PropertiesService.getScriptProperties();
    const isRunning = scriptProps.getProperty('IS_CURRENTLY_RUNNING') === 'true';
    const lastStartTime = scriptProps.getProperty('LAST_START_TIME');
    
    if (isRunning && lastStartTime) {
      const lastStartDate = new Date(lastStartTime);
      const minutesSinceStart = (new Date() - lastStartDate) / 1000 / 60;
      
      // Only clear if it's been running for more than 10 minutes
      if (minutesSinceStart > 10) {
        scriptProps.deleteProperty('IS_CURRENTLY_RUNNING');
        Logger.log(`🔄 Cleared potentially stuck run that started ${minutesSinceStart.toFixed(1)} minutes ago.`);
        return `Cleared potentially stuck run that started ${minutesSinceStart.toFixed(1)} minutes ago.`;
      } else {
        return `Current run has only been active for ${minutesSinceStart.toFixed(1)} minutes, which is not long enough to be considered stuck.`;
      }
    } else {
      return 'No currently running process found.';
    }
  } catch (e) {
    Logger.log('❌ Error clearing stuck run: ' + e.message);
    return 'Error: ' + e.message;
  }
}

// Function to enable advanced Drive API
function enableDriveApiAccess() {
  try {
    // Test the Drive advanced service by listing a file
    Drive.Files.list({maxResults: 1});
    Logger.log("✅ Drive API advanced service is available.");
  } catch (e) {
    Logger.log(`⚠️ Drive API advanced service not enabled: ${e.message}`);
    return false;
  }
  return true;
}

// Function to check if Drive API is enabled
function isDriveApiEnabled() {
  try {
    // Try to access the Drive API - if it fails, it's not enabled
    Drive.Files.get('test');
    return true;
  } catch (e) {
    // Check if the error message indicates the API is not enabled
    if (e.message.includes('Drive API has not been used in project before') || 
        e.message.includes('Drive is not defined')) {
      return false;
    }
    // For other errors, we'll assume the API is enabled but there's another issue
    return true;
  }
}

// Function to preserve file's modification date
function preserveFileDate(fileId, originalDate) {
  try {
    // Format date for Drive API
    const formattedDate = originalDate.toISOString();
    
    // Update file metadata with original modification date
    Drive.Files.patch({
      modifiedTime: formattedDate
    }, fileId, {
      fields: 'modifiedTime'
    });
    
    return true;
  } catch (e) {
    Logger.log(`⚠️ Error preserving file date: ${e.message}`);
    return false;
  }
}

// Add function to log operations
function logOperation(logsFolder, operation, details) {
  try {
    const timestamp = new Date().toISOString();
    const logEntry = {
      timestamp: timestamp,
      operation: operation,
      details: details
    };
    
    // Get or create today's log file
    const today = new Date().toISOString().split('T')[0];
    const logFileName = `log_${today}.json`;
    
    let logFile;
    const existingFiles = logsFolder.getFilesByName(logFileName);
    
    if (existingFiles.hasNext()) {
      logFile = existingFiles.next();
      // Safely parse existing logs, fallback to an empty array on error or empty content
      let existingLogs;
      try {
        const content = logFile.getBlob().getDataAsString();
        existingLogs = content.trim() ? JSON.parse(content) : [];
      } catch (parseErr) {
        Logger.log(`⚠️ Corrupt log JSON, starting fresh: ${parseErr.message}`);
        existingLogs = [];
      }
      existingLogs.push(logEntry);
      logFile.setContent(JSON.stringify(existingLogs, null, 2));
    } else {
      const newLogs = [logEntry];
      logFile = logsFolder.createFile(logFileName, JSON.stringify(newLogs, null, 2), 'application/json');
    }
    
    return true;
  } catch (e) {
    Logger.log(`⚠️ Error logging operation: ${e.message}`);
    return false;
  }
}

// Function to get existing queue chunks
function getQueueChunks(folder) {
  try {
    checkApiRateLimit();
    const chunkInfoFile = folder.getFilesByName('queue_chunks.json');
    if (chunkInfoFile.hasNext()) {
      try {
        const fileContent = chunkInfoFile.next().getBlob().getDataAsString();
        const chunkInfo = JSON.parse(fileContent) || [];
        
        // Validate each chunk exists
        const validChunks = [];
        for (const chunk of chunkInfo) {
          try {
            checkApiRateLimit();
            const chunkExists = folder.getFilesByName(chunk.filename).hasNext();
            if (chunkExists) {
              validChunks.push(chunk);
            } else {
              Logger.log(`⚠️ Chunk file missing: ${chunk.filename}, removing from tracking`);
            }
          } catch (e) {
            Logger.log(`⚠️ Error checking chunk ${chunk.filename}: ${e.message}`);
          }
        }
        
        // If we found valid chunks but some were invalid, update the tracking file
        if (validChunks.length > 0 && validChunks.length < chunkInfo.length) {
          Logger.log(`📊 Repairing queue chunks: Found ${validChunks.length} valid chunks out of ${chunkInfo.length}`);
          saveJsonToFile(folder, 'queue_chunks.json', validChunks);
          return validChunks;
        }
        
        return chunkInfo;
      } catch (parseError) {
        Logger.log(`❌ Error parsing queue chunks: ${parseError.message}`);
        return [];
      }
    }
    return [];
  } catch (e) {
    Logger.log(`❌ Error loading queue chunks: ${e.message}`);
    return [];
  }
}

// Function to create queue chunks from a large queue
function createQueueChunks(folder, queue) {
  try {
    if (queue.length === 0) return;
    
    // Calculate how many chunks we need
    const chunkCount = Math.ceil(queue.length / MAX_QUEUE_CHUNK_SIZE);
    Logger.log(`📊 Creating ${chunkCount} queue chunks for ${queue.length} items`);
    
    const chunkInfo = [];
    
    // Create each chunk
    for (let i = 0; i < chunkCount; i++) {
      const start = i * MAX_QUEUE_CHUNK_SIZE;
      const end = Math.min(start + MAX_QUEUE_CHUNK_SIZE, queue.length);
      const chunkItems = queue.slice(start, end);
      
      // Save chunk to a file
      const chunkName = `${QUEUE_CHUNK_PREFIX}${i}.json`;
      saveJsonToFile(folder, chunkName, chunkItems);
      
      // Keep track of source IDs for deduplication
      const sourceIds = chunkItems.map(item => item.sourceId);
      
      // Add to chunk info
      chunkInfo.push({
        index: i,
        filename: chunkName,
        itemCount: chunkItems.length,
        sourceIds: sourceIds,
        isProcessed: false
      });
    }
    
    // Save chunk info
    saveJsonToFile(folder, 'queue_chunks.json', chunkInfo);
    
    // Create or update empty main queue
    saveJsonToFile(folder, 'queue.json', []);
    
    return chunkInfo;
  } catch (e) {
    Logger.log(`❌ Error creating queue chunks: ${e.message}`);
    return null;
  }
}

// Function to add new items to queue chunks
function addItemsToQueueChunks(folder, newItems) {
  try {
    if (newItems.length === 0) return;
    
    // Get existing chunk info
    let chunkInfo = getQueueChunks(folder);
    
    // If no chunks exist yet, create new ones
    if (chunkInfo.length === 0) {
      return createQueueChunks(folder, newItems);
    }
    
    // Find the last chunk that isn't full
    let lastChunkIndex = chunkInfo.length - 1;
    let lastChunk = chunkInfo[lastChunkIndex];
    
    // If last chunk is full, create a new one
    if (lastChunk.itemCount >= MAX_QUEUE_CHUNK_SIZE) {
      lastChunkIndex = chunkInfo.length;
      lastChunk = {
        index: lastChunkIndex,
        filename: `${QUEUE_CHUNK_PREFIX}${lastChunkIndex}.json`,
        itemCount: 0,
        sourceIds: [],
        isProcessed: false
      };
      chunkInfo.push(lastChunk);
    }
    
    // Load the last chunk's items
    const lastChunkFile = folder.getFilesByName(lastChunk.filename);
    let lastChunkItems = [];
    if (lastChunkFile.hasNext()) {
      lastChunkItems = JSON.parse(lastChunkFile.next().getBlob().getDataAsString()) || [];
    }
    
    // How many items can we add to this chunk?
    const spaceInLastChunk = MAX_QUEUE_CHUNK_SIZE - lastChunkItems.length;
    
    // If we can fit all new items in the last chunk
    if (newItems.length <= spaceInLastChunk) {
      // Add all items to last chunk
      lastChunkItems = lastChunkItems.concat(newItems);
      saveJsonToFile(folder, lastChunk.filename, lastChunkItems);
      
      // Update chunk info
      lastChunk.itemCount = lastChunkItems.length;
      lastChunk.sourceIds = lastChunkItems.map(item => item.sourceId);
      chunkInfo[lastChunkIndex] = lastChunk;
    } else {
      // Fill the last chunk
      const itemsForLastChunk = newItems.slice(0, spaceInLastChunk);
      lastChunkItems = lastChunkItems.concat(itemsForLastChunk);
      saveJsonToFile(folder, lastChunk.filename, lastChunkItems);
      
      // Update last chunk info
      lastChunk.itemCount = lastChunkItems.length;
      lastChunk.sourceIds = lastChunkItems.map(item => item.sourceId);
      chunkInfo[lastChunkIndex] = lastChunk;
      
      // Create new chunks for remaining items
      const remainingItems = newItems.slice(spaceInLastChunk);
      const newChunks = createQueueChunks(folder, remainingItems);
      
      // Merge chunk info
      if (newChunks) {
        chunkInfo = chunkInfo.concat(newChunks);
      }
    }
    
    // Save updated chunk info
    saveJsonToFile(folder, 'queue_chunks.json', chunkInfo);
    
  } catch (e) {
    Logger.log(`❌ Error adding items to queue chunks: ${e.message}`);
  }
}

// Function to check for and clear stuck executions
function checkAndClearStuckExecution() {
  const scriptProps = PropertiesService.getScriptProperties();
  const isRunning = scriptProps.getProperty('IS_CURRENTLY_RUNNING') === 'true';
  const lastStartTime = scriptProps.getProperty('LAST_START_TIME');
  
  if (isRunning && lastStartTime) {
    const lastStartDate = new Date(lastStartTime);
    const minutesSinceStart = (new Date() - lastStartDate) / 1000 / 60;
    
    // If it's been running for more than 10 minutes, it's likely stuck
    if (minutesSinceStart > 10) {
      Logger.log(`⚠️ Detected potentially stuck execution from ${minutesSinceStart.toFixed(1)} minutes ago. Clearing.`);
      
      // Get logs folder for operation logging
      const logsFolderId = scriptProps.getProperty('LOGS_FOLDER_ID');
      if (logsFolderId) {
        try {
          const logsFolder = DriveApp.getFolderById(logsFolderId);
          logOperation(logsFolder, 'STUCK_EXECUTION_CLEARED', {
            lastStartTime: lastStartTime,
            minutesSinceStart: minutesSinceStart,
            clearedAt: new Date().toISOString()
          });
        } catch (e) {
          Logger.log(`❌ Error logging stuck execution: ${e.message}`);
        }
      }
      
      // Clear the running flag
      scriptProps.deleteProperty('IS_CURRENTLY_RUNNING');
      scriptProps.setProperty('LAST_EXECUTION_INTERRUPTED', 'true');
      scriptProps.setProperty('INTERRUPTION_TIME', new Date().toISOString());
    }
  }
}

// Add a periodic job state validation function
function validateJobState() {
  try {
    const scriptProps = PropertiesService.getScriptProperties();
    const queueFolderId = scriptProps.getProperty('QUEUE_FOLDER_ID');
    if (!queueFolderId) {
      Logger.log('❌ No active job found.');
      return false;
    }
    
    let queueFolder;
    try {
      queueFolder = DriveApp.getFolderById(queueFolderId);
    } catch (e) {
      Logger.log(`❌ Queue folder not accessible: ${e.message}`);
      return false;
    }
    
    const jsonFolderId = scriptProps.getProperty('JSON_FOLDER_ID');
    if (!jsonFolderId) {
      Logger.log('❌ JSON folder ID not found.');
      // Try to recover by finding the JSON folder
      const jsonFolders = queueFolder.getFoldersByName(JSON_FOLDER_NAME);
      if (jsonFolders.hasNext()) {
        const jsonFolder = jsonFolders.next();
        scriptProps.setProperty('JSON_FOLDER_ID', jsonFolder.getId());
        Logger.log('✅ Recovered JSON folder ID.');
      } else {
        Logger.log('❌ Could not recover JSON folder.');
        return false;
      }
    }
    
    return true;
  } catch (e) {
    Logger.log(`❌ Error validating job state: ${e.message}`);
    return false;
  }
}

// Enhanced recovery function for stuck jobs
function recoverStuckJob() {
  try {
    // Check if job validation passes
    if (!validateJobState()) {
      Logger.log('❌ Cannot recover job: validation failed.');
      return false;
    }
    
    const scriptProps = PropertiesService.getScriptProperties();
    const isRunning = scriptProps.getProperty('IS_CURRENTLY_RUNNING') === 'true';
    const lastStartTime = scriptProps.getProperty('LAST_START_TIME');
    
    if (isRunning && lastStartTime) {
      const lastStartDate = new Date(lastStartTime);
      const minutesSinceStart = (new Date() - lastStartDate) / (1000 * 60);
      
      // If it's been running for more than 10 minutes, it's likely stuck
      if (minutesSinceStart > 10) {
        Logger.log(`🔄 Recovering stuck job running for ${minutesSinceStart.toFixed(1)} minutes.`);
        
        // Clear the running flag
        scriptProps.setProperty('IS_CURRENTLY_RUNNING', 'false');
        scriptProps.setProperty('RECOVERED_FROM_STUCK', 'true');
        scriptProps.setProperty('RECOVERY_TIME', new Date().toISOString());
        
        // Create a new trigger to restart processing
        const triggers = ScriptApp.getProjectTriggers();
        let triggerExists = false;
        triggers.forEach(trigger => {
          if (trigger.getEventType() === ScriptApp.EventType.CLOCK && 
              trigger.getHandlerFunction() === 'processNextBatch') {
            triggerExists = true;
          }
        });
        
        if (!triggerExists) {
          ScriptApp.newTrigger('processNextBatch')
            .timeBased()
            .everyMinutes(COPY_TRIGGER_MINUTES)
            .create();
          
          Logger.log('✅ Created new trigger to restart processing.');
        }
        
        return true;
      }
    }
    
    Logger.log('ℹ️ No stuck job detected or job not running long enough to be considered stuck.');
    return false;
  } catch (e) {
    Logger.log(`❌ Error recovering stuck job: ${e.message}`);
    return false;
  }
}

// Function to automatically requeue failed items
function tryRequeueFailedItems(jsonFolder, failedItems) {
  if (!failedItems || failedItems.length === 0) {
    return;
  }
  
  try {
    Logger.log(`🔄 Auto-retrying ${failedItems.length} failed items`);
    
    // Use short hash IDs in logs
    const shortIds = failedItems.map(item => item.sourceId.slice(-6));
    
    // Load the current queue
    let queue = loadJsonFromFile(jsonFolder, 'queue.json') || [];
    
    // Check if we're using queue chunks
    const chunkInfo = getQueueChunks(jsonFolder);
    if (chunkInfo.length > 0) {
      // For chunked queues, add to the last non-full chunk or create a new one
      addItemsToQueueChunks(jsonFolder, failedItems);
    } else {
      // For regular queue, add to the main queue
      queue = failedItems.concat(queue);
      saveJsonToFile(jsonFolder, 'queue.json', queue);
    }
    
    // Clear the failed items list
    saveJsonToFile(jsonFolder, 'failed_items.json', []);
    
    // Update the logs with short IDs
    const logsFolderId = PropertiesService.getScriptProperties().getProperty('LOGS_FOLDER_ID');
    if (logsFolderId) {
      const logsFolder = DriveApp.getFolderById(logsFolderId);
      logOperation(logsFolder, 'FAILED_ITEMS_AUTO_REQUEUED', {
        failedItemCount: failedItems.length,
        shortIds: shortIds,
        timestamp: new Date().toISOString()
      });
    }
    
    // Log the action
    Logger.log(`✅ Successfully requeued ${failedItems.length} failed items for retry`);
  } catch (e) {
    Logger.log(`❌ Error requeuing failed items: ${e.message}`);
  }
}

// Add a function to the menu to manually requeue failed items
function requeueFailedItems() {
  try {
    // Validate the job state
    if (!validateJobState()) {
      const msg = 'No active job found or job state is invalid.';
      try { SpreadsheetApp.getUi().alert('Error', msg, SpreadsheetApp.getUi().ButtonSet.OK); }
      catch(e) { Logger.log(msg); }
      return;
    }
    
    const jsonFolderId = PropertiesService.getScriptProperties().getProperty('JSON_FOLDER_ID');
    const jsonFolder = DriveApp.getFolderById(jsonFolderId);
    
    // Load failed items
    const failedItems = loadJsonFromFile(jsonFolder, 'failed_items.json') || [];
    
    if (failedItems.length === 0) {
      const msg = 'There are no failed items to requeue.';
      try { SpreadsheetApp.getUi().alert('No Failed Items', msg, SpreadsheetApp.getUi().ButtonSet.OK); }
      catch(e) { Logger.log(msg); }
      return;
    }
    
    // Requeue the failed items
    tryRequeueFailedItems(jsonFolder, failedItems);
    
    // Show confirmation
    const confirmMsg = `Requeued ${failedItems.length} failed items for retry.`;
    try { SpreadsheetApp.getUi().alert('Success', confirmMsg, SpreadsheetApp.getUi().ButtonSet.OK); }
    catch(e) { Logger.log(confirmMsg); }
  } catch (e) {
    Logger.log(`❌ Error in requeueFailedItems: ${e.message}`);
    const errMsg = `Failed to requeue items: ${e.message}`;
    try { SpreadsheetApp.getUi().alert('Error', errMsg, SpreadsheetApp.getUi().ButtonSet.OK); }
    catch(err) { Logger.log(errMsg); }
  }
}

// Update the onOpen function to include the new requeue option
function onOpen() {
  try {
    const ui = SpreadsheetApp.getUi();
    ui.createMenu('Drive Copy')
      .addItem('Initialize Copy Job', 'initializeJob')
      .addItem('Check Job Status', 'showJobStatus')
      .addItem('Resume Failed Copies', 'resumeFailedCopies')
      .addItem('Requeue Failed Items', 'requeueFailedItems')
      .addItem('Reset Job', 'resetJob')
      .addItem('Recover Stuck Job', 'recoverStuckJob')
      .addToUi();
  } catch(e) {
    Logger.log('No Spreadsheet UI available; skipping onOpen menu setup.');
  }
}

// Function to show job status in a popup
function showJobStatus() {
  const status = getJobStatus();
  let message;
  if (status.status === 'NO_JOB') {
    message = 'No active job found. Please initialize a new copy job.';
  } else if (status.status === 'ERROR') {
    message = `Error: ${status.message}`;
  } else {
    message = `
Status: ${status.status}
Progress: ${status.progress}% (by item), ${status.bytesProgress}% (by bytes)
Total Items: ${status.totalItems}
Processed: ${status.processedItems}
Failed: ${status.failedItems}
Remaining: ${status.remainingItems}
Total Bytes: ${status.totalBytes || 0}
Processed Bytes: ${status.processedBytes || 0}
Failed Bytes: ${status.failedBytes || 0}
${status.pendingFolders > 0 ? 'Pending Folders: ' + status.pendingFolders : ''}
Start Time: ${new Date(status.startTime).toLocaleString()}
${status.endTime ? 'End Time: ' + new Date(status.endTime).toLocaleString() : ''}
Duration: ${status.durationMinutes} minutes
${status.isCurrentlyRunning ? '⚠️ Currently Running' : ''}
${status.potentiallyStuck ? '⚠️ Potentially Stuck' : ''}
    `;
  }
  try {
    const ui = SpreadsheetApp.getUi();
    ui.alert('Job Status', message, ui.ButtonSet.OK);
  } catch (e) {
    // Fallback when no spreadsheet UI is available
    Logger.log(message);
  }
}

// Add function to report progress
function reportProgress(metadata) {
  const itemsProgress = metadata.totalItems > 0 ? (metadata.processedItems / metadata.totalItems) * 100 : 0;
  const bytesProgress = metadata.totalBytes > 0 ? (metadata.processedBytes / metadata.totalBytes) * 100 : 0;
  Logger.log(`Progress: ${itemsProgress.toFixed(2)}% (Items), ${bytesProgress.toFixed(2)}% (Bytes)`);
}

function dumpLogs() {
  try {
    const queueFolders = DriveApp.getFoldersByName('_CopyQueueData');
    if (!queueFolders.hasNext()) {
      Logger.log('❌ Queue data folder not found.');
      return;
    }
    const queueFolder = queueFolders.next();
    
    const logsFolders = queueFolder.getFoldersByName('logs');
    if (!logsFolders.hasNext()) {
      Logger.log('❌ Logs subfolder not found.');
      return;
    }
    const logsFolder = logsFolders.next();
    
    const logFiles = logsFolder.getFiles();
    let combinedLogContent = `Combined Log Dump - ${new Date().toISOString()}\n========================================\n\n`;
    let fileCount = 0;
    
    while (logFiles.hasNext()) {
      const file = logFiles.next();
      fileCount++;
      combinedLogContent += `=== START: ${file.getName()} ===\n\n`;
      try {
        combinedLogContent += file.getBlob().getDataAsString();
      } catch (e) {
        combinedLogContent += `*** Error reading file ${file.getName()}: ${e.message} ***`;
      }
      combinedLogContent += `\n\n=== END: ${file.getName()} ===\n\n`;
    }
    
    if (fileCount === 0) {
       Logger.log('ℹ️ No log files found to combine.');
       return;
    }

    // Save the combined content to the main queue folder
    const outputFileName = 'combined_log_dump.txt';
    const existingDumps = queueFolder.getFilesByName(outputFileName);
    if (existingDumps.hasNext()) {
        existingDumps.next().setContent(combinedLogContent);
    } else {
        queueFolder.createFile(outputFileName, combinedLogContent, 'text/plain');
    }

    Logger.log(`✅ Combined ${fileCount} log files into ${outputFileName} in the _CopyQueueData folder.`);

  } catch (e) {
      Logger.log(`❌ Error dumping logs: ${e.message}`);
  }
}

/**
 * Verify integrity of the copy by walking both source and destination trees,
 * reusing checkApiRateLimit and MAX_TRAVERSAL_DEPTH constants.
 * Logs missing, extra, and mismatched files (size/md5).
 */
function verifyCopyIntegrity() {
  const src = DriveApp.getFolderById(SOURCE_FOLDER_ID);
  const dstRoot = getFolderByPath(DESTINATION_PATH);
  const dst = dstRoot.getFoldersByName(src.getName()).hasNext()
    ? dstRoot.getFoldersByName(src.getName()).next()
    : null;

  if (!dst) {
    Logger.log('❌ Destination folder does not exist; cannot verify integrity.');
    return;
  }

  const srcFiles = {};
  const dstFiles = {};

  _collectFolderFiles(src, '', srcFiles);
  _collectFolderFiles(dst, '', dstFiles);

  const srcPaths = Object.keys(srcFiles);
  const dstPaths = Object.keys(dstFiles);

  Logger.log(`Scanned ${srcPaths.length} source files, ${dstPaths.length} destination files`);

  const missing = srcPaths.filter(p => !dstFiles.hasOwnProperty(p));
  const extra   = dstPaths.filter(p => !srcFiles.hasOwnProperty(p));
  const mismatched = srcPaths.filter(p => {
    if (!dstFiles.hasOwnProperty(p)) return false;
    const a = srcFiles[p];
    const b = dstFiles[p];
    // size or md5 difference
    return a.size !== b.size || (a.md5 && b.md5 && a.md5 !== b.md5);
  });

  // Sort for consistent reporting
  missing.sort();
  extra.sort();
  mismatched.sort();

  Logger.log(`🔍 Missing files (${missing.length}): ${missing.join(', ')}`);
  Logger.log(`🔍 Extra files   (${extra.length}): ${extra.join(', ')}`);
  Logger.log(`🔍 Mismatched   (${mismatched.length}): ${mismatched.join(', ')}`);

  // Write report JSON to logs folder if available
  const logsFolderId = PropertiesService.getScriptProperties().getProperty('LOGS_FOLDER_ID');
  if (logsFolderId) {
    const logsFolder = DriveApp.getFolderById(logsFolderId);
    // Save with a timestamped filename so we don't overwrite old reports
    const ts = new Date().toISOString().replace(/[:.]/g, '-');
    const reportName = `integrity_report_${ts}.json`;
    saveJsonToFile(logsFolder, reportName, { missing, extra, mismatched });
    Logger.log(`📁 Integrity report saved to ${reportName}`);
  }
}

/**
 * Recursive helper to collect files under a folder, respecting rate limits and depth.
 * @param {Folder} folder  The DriveApp Folder to traverse
 * @param {string} basePath  The path prefix (relative)
 * @param {Object} fileMap  Map from relative path to metadata
 * @param {number=} depth  Current depth (for MAX_TRAVERSAL_DEPTH)
 */
function _collectFolderFiles(folder, basePath, fileMap, depth = 0) {
  if (depth > MAX_TRAVERSAL_DEPTH) return;
  checkApiRateLimit();

  // List files in this folder
  const files = folder.getFiles();
  while (files.hasNext()) {
    checkApiRateLimit();
    const f = files.next();
    const rel = (basePath ? basePath + '/' : '') + f.getName();
    fileMap[rel] = {
      size: f.getSize(),
      md5: f.getMd5Checksum ? f.getMd5Checksum() : null
    };
  }

  // Recurse into subfolders
  const subs = folder.getFolders();
  while (subs.hasNext()) {
    checkApiRateLimit();
    const sf = subs.next();
    const path = (basePath ? basePath + '/' : '') + sf.getName();
    _collectFolderFiles(sf, path, fileMap, depth + 1);
  }
}

/**
 * Compare any two folders by ID without using log files.
 * @param {string} srcFolderId ID of the source folder
 * @param {string} dstFolderId ID of the destination folder
 * @returns {{missing:string[], extra:string[], mismatched:string[]}} Diff report
 */
function compareTwoFolders(srcFolderId, dstFolderId) {
  const src = DriveApp.getFolderById(srcFolderId);
  const dst = DriveApp.getFolderById(dstFolderId);
  const srcFiles = {};
  const dstFiles = {};
  _collectFolderFiles(src, '', srcFiles);
  _collectFolderFiles(dst, '', dstFiles);

  const srcPaths = Object.keys(srcFiles);
  const dstPaths = Object.keys(dstFiles);
  Logger.log(`Scanned ${srcPaths.length} source files, ${dstPaths.length} destination files`);

  const missing = srcPaths.filter(p => !dstFiles.hasOwnProperty(p)).sort();
  const extra = dstPaths.filter(p => !srcFiles.hasOwnProperty(p)).sort();
  const mismatched = srcPaths.filter(p => {
    if (!dstFiles.hasOwnProperty(p)) return false;
    const a = srcFiles[p];
    const b = dstFiles[p];
    return a.size !== b.size || (a.md5 && b.md5 && a.md5 !== b.md5);
  }).sort();

  Logger.log(`🔍 Missing (${missing.length}): ${missing.join(', ')}`);
  Logger.log(`🔍 Extra   (${extra.length}): ${extra.join(', ')}`);
  Logger.log(`🔍 Mismatch(${mismatched.length}): ${mismatched.join(', ')}`);

  return { missing, extra, mismatched };
}

function runCompareForCopyJob() {
  // 1) Source is already known
  const srcId = SOURCE_FOLDER_ID;

  // 2) Resolve the dest‐root folder by path
  const destRoot = getFolderByPath(DESTINATION_PATH);
  // 3) Inside that, find the subfolder named the same as the source
  const srcName = DriveApp.getFolderById(srcId).getName();
  const destIter = destRoot.getFoldersByName(srcName);
  if (!destIter.hasNext()) {
    Logger.log('❌ Cannot find destination subfolder "%s" under "%s".', srcName, DESTINATION_PATH);
    return;
  }
  const dstId = destIter.next().getId();

  // 4) Run the compare
  const diff = compareTwoFolders(srcId, dstId);
  Logger.log('Compare result:\n' + JSON.stringify(diff, null, 2));
}