# CSV Filtering System for GCU TrackMan Data

This system allows you to prevent problematic CSV files from being downloaded during the automated TrackMan data sync process.

## Problem This Solves

Sometimes TrackMan operators make mistakes during games (incorrect pitcher assignments, wrong pitch types, etc.). Even though they correct the CSV files later, the bad versions remain on the FTP server and get downloaded by the automation script, causing issues in your app.

## How It Works

The filtering system checks each CSV file against an exclusion list before downloading. If a file matches any pattern in the exclusion list, it will be skipped entirely.

## Files Added/Modified

1. **`csv_filter_utils.R`** - Core filtering functions
2. **`manage_exclusions.R`** - Management script for adding/removing exclusions
3. **`automated_data_sync.R`** - Modified to include filtering logic
4. **`data/csv_exclusions.txt`** - Text file containing exclusion patterns (created automatically)

## Usage

### Quick Commands

```bash
# List current exclusions
Rscript manage_exclusions.R list

# Add a bad file to exclusion list
Rscript manage_exclusions.R add "BadData_2025-10-27_Game.csv" "Incorrect pitcher assignments"

# Remove a file from exclusion list
Rscript manage_exclusions.R remove "BadData_2025-10-27_Game.csv"

# Check recent downloads (last 24 hours)
Rscript manage_exclusions.R recent

# Interactive menu
Rscript manage_exclusions.R
```

### When TrackMan Operators Make Mistakes

1. **Identify the bad file**: Look at recent downloads or check your app for problematic data
2. **Add to exclusion list**: Use the filename or a partial pattern that identifies the bad file
3. **Re-run sync**: The next automated sync will skip the bad file

### Example Workflow

```bash
# You notice bad data from October 27th game
Rscript manage_exclusions.R recent

# Add the specific bad file
Rscript manage_exclusions.R add "Game_2025-10-27_BadPitcher.csv" "Wrong pitcher assigned to multiple plays"

# Verify it was added
Rscript manage_exclusions.R list

# Next sync will skip this file automatically
```

### Pattern Matching

The system supports both exact filename matches and partial pattern matches:

- `BadFile.csv` - Matches exactly "BadFile.csv"
- `BadData_2025-10-27` - Matches any file containing this text
- `Game_Wrong` - Matches any file with "Game_Wrong" in the name

## Manual Management

You can also directly edit `data/csv_exclusions.txt`:

```
# CSV Exclusion List for GCU TrackMan Data Sync
# Add filenames or patterns (one per line) that should be excluded from automatic sync
# Lines starting with # are comments and will be ignored

# October 27 bad game data - wrong pitcher assignments
Game_2025-10-27_15:30.csv
BadData_Pitcher_Smith.csv

# Practice session with equipment malfunction
Practice_2025-10-26_Malfunction
```

## What Happens During Sync

When the automation runs:

1. Each CSV file is checked against the exclusion list
2. If a match is found, the file is skipped with a message: `EXCLUDED: filename.csv (matches pattern: pattern)`
3. Good files are downloaded normally
4. The sync log will show which files were excluded

## Backup and Safety

- A backup of the original `automated_data_sync.R` was created automatically
- The exclusion system only prevents downloads; it doesn't delete existing files
- You can remove exclusions at any time to allow files to be downloaded again

## Testing

The system has been tested to ensure:
- Correct files are still downloaded
- Bad files are properly excluded
- The exclusion list management works correctly
- The filtering doesn't break existing functionality

## Support

If you need to disable filtering temporarily, you can comment out the filtering check in the `download_csv` function in `automated_data_sync.R`:

```r
# if (should_exclude_csv(filename)) {
#   return(FALSE)
# }
```
